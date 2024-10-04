use std::collections::BTreeMap;
use std::error::Error;
use std::fmt;
use std::fmt::Formatter;
use std::future::Future;
use std::pin::{pin, Pin};
use std::time::Duration;
use mysql_async::{Conn, Opts, OptsBuilder, Pool, Transaction, TxOpts, Value};
use mysql_async::prelude::{FromRow, Query, Queryable, WithParams};
use tokio::{fs, join};
use tokio::sync::mpsc::{self, UnboundedReceiver, UnboundedSender};
use std::str::FromStr;
use futures::stream::FuturesUnordered;
use futures::{Stream, StreamExt};

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
enum WorkerCmd {
  Continue,
  Fail,
  // shard id, policy, key-to-set
  Set(u32, MigrationPolicy, Option<String>),
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
enum Action {
  Goto(String),
  Start(u32, MigrationPolicy, String),
  Fail,
  End,
}

impl Action {
  pub fn parse(line: &str) -> (usize, Self) {
    let (prefix, line) = line.split_once(' ').unwrap();
    let action: Self = match line {
      "end" => Self::End,
      "fail" => Self::Fail,
      s if s.starts_with("goto ") => Self::Goto(String::from(&s[5..])),
      s if s.starts_with("start ") => {
        let mut args = s[6..].split(' ');
        let shard_id: u32 = args.next().unwrap().parse().unwrap();
        let policy: MigrationPolicy = args.next().unwrap().parse().unwrap();
        let value: String = args.next().unwrap().parse().unwrap();
        Self::Start(shard_id, policy, value)
      }
      s => panic!("invalid action {s:?}"),
    };
    let id = usize::from_str(prefix).expect("line must be valid");
    (id, action)
  }
}


#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
struct ActionList(Vec<(usize, Action)>);

impl ActionList {
  pub fn parse(text: &str) -> Self {
    let mut actions = Vec::new();
    for line in text.lines() {
      let line = line.trim();
      if line.is_empty() {
        continue;
      }
      actions.push(Action::parse(line));
    }
    Self(actions)
  }
}

#[tokio::main]
async fn main() {
  let input = fs::read_to_string("actions.txt").await.expect("actions.txt is valid");
  let actions = ActionList::parse(&input);

  const WORKER_COUNT: usize = 3;
  let mut worker_rx = Vec::new();
  let mut worker_tx = Vec::new();

  for _ in 0..WORKER_COUNT {
    let (tx, rx) = mpsc::unbounded_channel::<WorkerCmd>();
    worker_tx.push(tx);
    worker_rx.push(rx);
  }
  let (dtx, drx) = mpsc::unbounded_channel::<(usize, &'static str)>();
  let mut workers = worker_rx.into_iter().enumerate().map(|(id, rx)| worker(id, dtx.clone(), rx)).collect::<Vec<_>>();

  let d = driver(&worker_tx, drx, actions);

  let mut all_futs: FuturesUnordered<Pin<Box<dyn Future<Output = Result<String, Box<dyn Error>>>>>> = FuturesUnordered::new();
  for w in workers {
    all_futs.push(Box::pin(w));
  }
  all_futs.push(Box::pin(d));

  let mut all_futs = pin!(all_futs);
  while let Some(name) = all_futs.next().await {
    eprintln!("future complete: {name:?}");
  }
}

fn get_admin_pool() -> Pool {
  let opts: Opts = OptsBuilder::default().user(Some("root")).pass(Some("dev")).into();
  Pool::new(opts)
}

fn get_worker_pool(db_name: &str) -> Pool {
  let opts: Opts = OptsBuilder::default().user(Some("root")).pass(Some("dev")).db_name(Some(db_name)).into();
  Pool::new(opts)
}

async fn driver(wtx: &[UnboundedSender<WorkerCmd>], mut drx: UnboundedReceiver<(usize, &'static str)>, action_list: ActionList) -> Result<String, Box<dyn Error>> {
  eprintln!("driver: ready");
  reset_db("legacy").await?;
  reset_db("fresh").await?;
  eprintln!("driver: db_prepared");

  for (target, action) in action_list.0 {
    eprintln!("driver: target={target} action={action:?}");
    match action {
      Action::End => {
        let worker = &wtx[target];
        worker.send(WorkerCmd::Continue).expect_err("worker is closed");
      }
      Action::Goto(step) => {
        let worker = &wtx[target];
        loop {
          worker.send(WorkerCmd::Continue).expect("worker is open");
          if let Some((t, ev)) = drx.recv().await {
            eprintln!("{t}: {ev}");
            if t != target {
              panic!("unexpected event source");
            }
            if ev == &step {
              break;
            }
          } else {
            break;
          }
        }
      }
      Action::Start(shard, policy, value) => {
        let worker = &wtx[target];
        worker.send(WorkerCmd::Set(shard, policy, Some(value))).expect("worker is open");
        if let Some((t, ev)) = drx.recv().await {
          eprintln!("{t}: {ev}");
          if t != target {
            panic!("unexpected event source");
          }
          assert_eq!(ev, "start");
        } else {
          break;
        }
      }
      Action::Fail => {
        let worker = &wtx[target];
        worker.send(WorkerCmd::Fail).expect("worker is open");
        while let Some((t, ev)) = drx.recv().await {
          eprintln!("{t}: {ev}");
        }
      }
    }
  }
  eprintln!("driver: done");
  Ok(String::from("driver"))
}

async fn reset_db(db_name: &str) -> Result<(), Box<dyn Error>> {
  let pool = get_admin_pool();
  let mut admin_conn: Conn = pool.get_conn().await.expect("Failed to connect to MariaDB");
  // language=mariadb
  admin_conn.query_drop(format!("drop database if exists `{}`;", db_name)).await?;
  // language=mariadb
  admin_conn.query_drop(format!("create database `{}`;", db_name)).await?;
  let worker_pool = get_worker_pool(db_name);
  let mut worker_conn: Conn = worker_pool.get_conn().await.expect("Failed to connect to MariaDB");
  let schema = include_str!("./schema.sql");
  worker_conn.query_drop(schema).await?;
  Ok(())
}

macro_rules! wait_on {
    ($tx: expr, $rx: expr, $worker_id: expr, $step:literal) => {
      match $rx.recv().await.expect("driver is open") {
        WorkerCmd::Continue => {},
        WorkerCmd::Fail => {
          return Err(String::from("fail").into())
        },
        WorkerCmd::Set(..) => { panic!("unexpected `Set` in `wait_on`")},
      }
      $tx.send(($worker_id, $step)).expect("driver is open");
    };
}

async fn worker(worker_id: usize, tx: UnboundedSender<(usize, &'static str)>, mut rx: UnboundedReceiver<WorkerCmd>) -> Result<String, Box<dyn Error>> {
  let mut shard_id = 1;
  let mut policy = MigrationPolicy::Migrate;
  let mut key: Option<String> = None;

  match rx.recv().await.expect("driver is open") {
    WorkerCmd::Continue => {}
    WorkerCmd::Set(new_shard_id, new_policy, new_key) => {
      shard_id = new_shard_id;
      policy = new_policy;
      key = new_key;
    }
    WorkerCmd::Fail => {
      return Err(String::from("fail").into())
    }
  }
  tx.send((worker_id, "start")).expect("driver is open");

  let mut db_tx = get_db_tx(worker_id, &tx, &mut rx, &get_worker_pool("legacy"), &get_worker_pool("fresh"), policy, shard_id).await?;
  wait_on!(tx, rx, worker_id, "get_db_tx");
  eprintln!("{worker_id}: {db_tx:?}");

  wait_on!(tx, rx, worker_id, "work_start");
  match key {
    Some(key) => {
      let (db_tx, is_fresh) = match &mut db_tx {
        DbTx::Fresh(db_tx) => (db_tx, true),
        DbTx::Legacy(db_tx) => (db_tx, false),
      };

      wait_on!(tx, rx, worker_id, "set_shard_entry_start");
      set_shard_entry(db_tx, shard_id, key, String::from("foo")).await?;
      wait_on!(tx, rx, worker_id, "set_shard_entry_end");

      wait_on!(tx, rx, worker_id, "bump_shard_revision_start");
      bump_shard_revision(db_tx, shard_id, is_fresh).await?;
      wait_on!(tx, rx, worker_id, "bump_shard_revision_end");
    }
    None => {
      // no action
    }
  }
  wait_on!(tx, rx, worker_id, "work_end");

  match db_tx {
    DbTx::Fresh(db_tx) => { db_tx.commit().await?; }
    DbTx::Legacy(db_tx) => { db_tx.commit().await?; }
  }

  wait_on!(tx, rx, worker_id, "end");
  Ok(format!("worker{worker_id}"))
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
enum MigrationPolicy {
  DoNotMigrate,
  Migrate,
}

impl FromStr for MigrationPolicy {
  type Err = ();

  fn from_str(s: &str) -> Result<Self, Self::Err> {
    match s {
      "DoNotMigrate" => Ok(MigrationPolicy::DoNotMigrate),
      "Migrate" => Ok(MigrationPolicy::Migrate),
      _ => Err(()),
    }
  }
}

enum DbTx {
  Legacy(Transaction<'static>),
  Fresh(Transaction<'static>),
}

impl fmt::Debug for DbTx {
  fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
    match self {
      Self::Legacy(_) => f.write_str("DbTx::Legacy(..)"),
      Self::Fresh(_) => f.write_str("DbTx::Fresh(..)"),
    }
  }
}

async fn get_db_tx(worker_id: usize, tx: &UnboundedSender<(usize, &'static str)>, rx: &mut UnboundedReceiver<WorkerCmd>, legacy_pool: &Pool, fresh_pool: &Pool, policy: MigrationPolicy, shard_id: u32) -> Result<DbTx, Box<dyn Error>> {
  wait_on!(tx, rx, worker_id, "get_db_tx:fresh_tx");
  let mut fresh_tx = fresh_pool.start_transaction(TxOpts::default()).await?;
  wait_on!(tx, rx, worker_id, "get_db_tx:fresh_get_meta_or_default");
  let fresh_meta = get_meta_or_default(&mut fresh_tx, shard_id).await?;
  eprintln!("{worker_id}: /get_db_tx:fresh_get_meta_or_default: {fresh_meta:?}");
  if fresh_meta.is_migrated {
    eprintln!("{worker_id}: get_db_tx:done with fresh tx");
    return Ok(DbTx::Fresh(fresh_tx));
  }
  wait_on!(tx, rx, worker_id, "get_db_tx:fresh_tx_commit");
  fresh_tx.commit().await?;
  wait_on!(tx, rx, worker_id, "get_db_tx:match_policy");
  match policy {
    MigrationPolicy::DoNotMigrate => {
      wait_on!(tx, rx, worker_id, "get_db_tx:legacy_tx");
      let mut legacy_tx = legacy_pool.start_transaction(TxOpts::default()).await?;
      eprintln!("{worker_id}: get_db_tx:legacy_get_meta_or_default");
      let legacy_meta = get_meta_or_default(&mut legacy_tx, shard_id).await?;
      eprintln!("{worker_id}: /get_db_tx:legacy_get_meta_or_default: {legacy_meta:?}");
      if !fresh_meta.is_migrated {
        eprintln!("{worker_id}: get_db_tx:done with legacy tx");
        return Ok(DbTx::Legacy(legacy_tx));
      }
      wait_on!(tx, rx, worker_id, "get_db_tx:legacy_tx_commit");
      legacy_tx.commit().await?;
      wait_on!(tx, rx, worker_id, "get_db_tx:fresh_tx2");
      let fresh_tx2 = fresh_pool.start_transaction(TxOpts::default()).await?;
      Ok(DbTx::Fresh(fresh_tx2))
    }
    MigrationPolicy::Migrate => {
      wait_on!(tx, rx, worker_id, "get_db_tx:migrate_shard");
      migrate_shard(worker_id, tx, rx, &legacy_pool, &fresh_pool, shard_id).await?;
      wait_on!(tx, rx, worker_id, "get_db_tx:fresh_tx_migrated");
      let fresh_tx_migrated = fresh_pool.start_transaction(TxOpts::default()).await?;
      Ok(DbTx::Fresh(fresh_tx_migrated))
    }
  }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, FromRow)]
struct ShardMeta {
  shard_id: u32,
  revision: u32,
  is_migrated: bool,
}

async fn get_meta_or_default(tx: &mut Transaction<'_>, shard_id: u32) -> Result<ShardMeta, Box<dyn Error>> {
  // language=mariadb
  let row: Option<ShardMeta> = tx.exec_first(r"select shard_id, revision, is_migrated from shard_meta where shard_id = ?;", vec![shard_id]).await?;
  Ok(row.unwrap_or(ShardMeta { shard_id, revision: 0, is_migrated: false }))
}

async fn migrate_shard(worker_id: usize, tx: &UnboundedSender<(usize, &'static str)>, rx: &mut UnboundedReceiver<WorkerCmd>, legacy_pool: &Pool, fresh_pool: &Pool, shard_id: u32) -> Result<(), Box<dyn Error>> {
  // step 1: read legacy
  wait_on!(tx, rx, worker_id, "migrate_shard:legacy_read_tx");
  let mut legacy_read_tx = legacy_pool.start_transaction(TxOpts::default()).await?;
  wait_on!(tx, rx, worker_id, "migrate_shard:legacy_read_get_meta_or_default");
  let legacy_read_meta = get_meta_or_default(&mut legacy_read_tx, shard_id).await?;
  eprintln!("{worker_id}: /migrate_shard:legacy_read_get_meta_or_default: {legacy_read_meta:?}");
  if legacy_read_meta.is_migrated {
    wait_on!(tx, rx, worker_id, "migrate_shard:legacy_read_tx_commit");
    legacy_read_tx.commit().await?;
    eprintln!("{worker_id}: /migrate_shard:legacy_read_tx_commit");
  } else {
    wait_on!(tx, rx, worker_id, "migrate_shard:legacy_read_get_shard_data");
    let legacy_shard_data = get_shard_data(&mut legacy_read_tx, shard_id).await?;
    eprintln!("{worker_id}: /migrate_shard:legacy_read_get_shard_data: {legacy_shard_data:?}");
    wait_on!(tx, rx, worker_id, "migrate_shard:legacy_read_tx_commit_after_read");
    legacy_read_tx.commit().await?;
    eprintln!("{worker_id}: /migrate_shard:legacy_read_tx_commit_after_read");

    // step 2: prepare data
    eprintln!("{worker_id}: migrate_shard:fresh_prepare_tx");
    let mut fresh_prepare_tx = fresh_pool.start_transaction(TxOpts::default()).await?;
    eprintln!("{worker_id}: migrate_shard:fresh_prepare_get_meta_or_default");
    let fresh_prepare_meta = get_meta_or_default(&mut fresh_prepare_tx, shard_id).await?;
    eprintln!("{worker_id}: /migrate_shard:fresh_prepare_get_meta_or_default: {fresh_prepare_meta:?}");
    if fresh_prepare_meta.is_migrated {
      eprintln!("{worker_id}: migrate_shard:done with fresh prepare tx");
      return Ok(());
    }
    eprintln!("{worker_id}: migrate_shard:fresh_prepare_tx_set_data");
    set_shard_data(&mut fresh_prepare_tx, legacy_read_meta.revision, legacy_shard_data).await?;
    eprintln!("{worker_id}: migrate_shard:fresh_prepare_tx_commit");
    fresh_prepare_tx.commit().await?;
    eprintln!("{worker_id}: /migrate_shard:fresh_prepare_tx_commit");

    // step 3: vote
    eprintln!("{worker_id}: migrate_shard:legacy_vote_tx");
    let mut legacy_vote_tx = legacy_pool.start_transaction(TxOpts::default()).await?;
    eprintln!("{worker_id}: migrate_shard:set_is_migrated_if_revision");
    let vote_result = set_is_migrated_if_revision(&mut legacy_vote_tx, shard_id, legacy_read_meta.revision).await?;
    eprintln!("{worker_id}: /migrate_shard:set_is_migrated_if_revision: {vote_result:?}");
    eprintln!("{worker_id}: migrate_shard:legacy_vote_tx_commit");
    legacy_vote_tx.commit().await?;
    eprintln!("{worker_id}: /migrate_shard:legacy_vote_tx_commit");
    if vote_result == MigrationResult::NotFound {
      eprintln!("{worker_id}: migrate_shard:legacy_vote_tx_race_error");
      return Err(String::from("raced migration").into());
    }
  }
  // step 4: ack
  eprintln!("{worker_id}: migrate_shard:fresh_ack_tx");
  let mut fresh_ack_tx = fresh_pool.start_transaction(TxOpts::default()).await?;
  eprintln!("{worker_id}: migrate_shard:fresh_ack_tx_set_is_migrated");
  set_is_migrated(&mut fresh_ack_tx, shard_id, legacy_read_meta.revision).await?;
  eprintln!("{worker_id}: /migrate_shard:fresh_ack_tx_set_is_migrated");
  wait_on!(tx, rx, worker_id, "migrate_shard:fresh_ack_tx_commit");
  fresh_ack_tx.commit().await?;
  eprintln!("{worker_id}: /migrate_shard:fresh_ack_tx_commit");

  Ok(())
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct ShardData {
  shard_id: u32,
  data: BTreeMap<String, String>,
}

async fn get_shard_data(tx: &mut Transaction<'_>, shard_id: u32) -> Result<ShardData, Box<dyn Error>> {
  // language=mariadb
  let rows: Vec<(String, String)> = tx.exec(r"select name, value from shard_value_map where shard_id = ?;", vec![shard_id]).await?;
  let data = BTreeMap::from_iter(rows.into_iter());
  Ok(ShardData { shard_id, data })
}

async fn set_shard_data(tx: &mut Transaction<'_>, shard_revision: u32, data: ShardData) -> Result<(), Box<dyn Error>> {
  // language=mariadb
  tx.exec_drop(r"delete from shard_meta where shard_id = ?;", vec![data.shard_id]).await?;
  // language=mariadb
  tx.exec_drop(r"delete from shard_value_map where shard_id = ?;", vec![data.shard_id]).await?;
  // language=mariadb
  tx.exec_drop(r"insert into shard_meta(shard_id, revision, is_migrated) values (?, ?, false);", vec![data.shard_id, shard_revision]).await?;
  for (key, value) in data.data {
    // language=mariadb
    tx.exec_drop(r"insert into shard_value_map(entry_id, shard_id, name, value) values (default, ?, ?, ?);", vec![Value::Int(i64::from(data.shard_id)), Value::Bytes(key.into_bytes()), Value::Bytes(value.into_bytes())]).await?;
  }
  Ok(())
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
enum MigrationResult {
  Updated,
  NotFound,
}

async fn set_is_migrated_if_revision(tx: &mut Transaction<'_>, shard_id: u32, shard_revision: u32) -> Result<MigrationResult, Box<dyn Error>> {
  // language=mariadb
  let cur_meta = tx.exec_first::<(u32, bool), _, _>("select revision, is_migrated from shard_meta where shard_id = ?", vec![shard_id]).await?;
  match cur_meta {
    None => {
      // language=mariadb
      tx.exec_drop(r"insert into shard_meta(shard_id, revision, is_migrated) values (?, ?, true);", vec![shard_id, shard_revision]).await?;
      Ok(MigrationResult::Updated)
    }
    Some((revision, is_migrated)) => {
      if revision != shard_revision {
        return Ok(MigrationResult::NotFound);
      }
      if !is_migrated {
        // language=mariadb
        tx.exec_drop(r"update shard_meta set is_migrated = true where shard_id = ? and revision = ?;", vec![shard_id, shard_revision]).await?;
        let affected = tx.affected_rows();
        assert!(affected == 1);
      }
      Ok(MigrationResult::Updated)
    }
  }
}

async fn set_is_migrated(tx: &mut Transaction<'_>, shard_id: u32, default_shard_revision: u32) -> Result<(), Box<dyn Error>> {
  // language=mariadb
  tx.exec_drop(r"insert into shard_meta(shard_id, revision, is_migrated) values (?, ?, true) on duplicate key update is_migrated = true;", vec![shard_id, default_shard_revision]).await?;
  let affected = tx.affected_rows();
  assert!(affected > 0);
  Ok(())
}

async fn set_shard_entry(tx: &mut Transaction<'_>, shard_id: u32, key: String, value: String) -> Result<(), Box<dyn Error>> {
  // language=mariadb
  tx.exec_drop(r"insert into shard_value_map(entry_id, shard_id, name, value) values (default, ?, ?, ?) on duplicate key update value = values(value);", vec![Value::Int(shard_id.into()), Value::Bytes(key.into_bytes()), Value::Bytes(value.into_bytes())]).await?;
  let affected = tx.affected_rows();
  assert!(affected > 0);
  Ok(())
}

async fn bump_shard_revision(tx: &mut Transaction<'_>, shard_id: u32, expected_is_migrated: bool) -> Result<(), Box<dyn Error>> {
  // language=mariadb
  let cur_meta = tx.exec_first::<(u32, bool), _, _>("select revision, is_migrated from shard_meta where shard_id = ?", vec![shard_id]).await?;
  match cur_meta {
    None => {
      // language=mariadb
      tx.exec_drop(r"insert into shard_meta(shard_id, revision, is_migrated) values (?, ?, ?);", vec![Value::Int(shard_id.into()), Value::Int(1), Value::Int(expected_is_migrated.into())]).await?;
      let affected = tx.affected_rows();
      assert!(affected == 1);
      Ok(())
    }
    Some((old_revision, _is_migrated)) => {
      let new_revision = old_revision + 1;
      // language=mariadb
      tx.exec_drop(r"update shard_meta set revision = ? where shard_id = ? and revision = ?;", vec![Value::Int(new_revision.into()), Value::Int(shard_id.into()), Value::Int(old_revision.into())]).await?;
      let affected = tx.affected_rows();
      assert!(affected == 1);
      Ok(())
    }
  }
}
