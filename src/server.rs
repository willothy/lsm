// use std::{io::Write, path::PathBuf, str::FromStr, time::Duration};
//
// use anyhow::Context;
// use clap::Parser;
// use gmf::server::gmf_server::GmfServer;
// use tonic::client::GrpcService;
//
// pub mod rpc {
//     mod generated {
//         tonic::include_proto!("db");
//     }
//
//     pub use generated::{
//         DeleteRequest, DeleteResponse, GetRequest, GetResponse, PutRequest, PutResponse,
//     };
//
//     pub use generated::database_client::DatabaseClient;
//
//     pub use generated::database_server::{Database, DatabaseServer};
// }
//
// pub struct DbServer {}
//
// #[tonic::async_trait]
// impl rpc::Database for DbServer {
//     async fn get(
//         &self,
//         request: tonic::Request<rpc::GetRequest>,
//     ) -> std::result::Result<tonic::Response<rpc::GetResponse>, tonic::Status> {
//         todo!()
//     }
//
//     async fn put(
//         &self,
//         request: tonic::Request<rpc::PutRequest>,
//     ) -> std::result::Result<tonic::Response<rpc::PutResponse>, tonic::Status> {
//         todo!()
//     }
//
//     async fn delete(
//         &self,
//         request: tonic::Request<rpc::DeleteRequest>,
//     ) -> std::result::Result<tonic::Response<rpc::DeleteResponse>, tonic::Status> {
//         todo!()
//     }
// }
//
// #[derive(Debug, Clone, clap::Parser)]
// struct Cli {
//     data_dir: PathBuf,
// }
//
// pub fn main() -> anyhow::Result<()> {
//     let args = Cli::parse();
//
//     std::fs::create_dir_all(&args.data_dir)?;
//
//     let pidfile_path = args.data_dir.join("mintdb.pid");
//
//     if pidfile_path.try_exists().is_ok_and(|readable| readable) {
//         return Err(anyhow::anyhow!(
//             "PID file already exists at {:?}. Is another instance running?",
//             pidfile_path
//         ));
//     }
//
//     let me = procfs::process::Process::myself()?;
//
//     let mut pidfile = std::fs::File::create(&pidfile_path)?;
//
//     pidfile.write_all(format!("{}", me.pid).as_bytes())?;
//     pidfile.flush()?;
//
//     let database = glommio::LocalExecutorBuilder::new(glommio::Placement::Unbound)
//         .name("server-executor")
//         .spawn(|| async move {
//             let executor = glommio::executor();
//
//             let request_queue = executor.create_task_queue(
//                 glommio::Shares::Static(10),
//                 glommio::Latency::Matters(Duration::from_millis(10)),
//                 "api-requests",
//             );
//
//             let background_queue = executor.create_task_queue(
//                 glommio::Shares::Static(1),
//                 glommio::Latency::NotImportant,
//                 "background-tasks",
//             );
//
//             // executor.create_task_queue()
//             //
//         })
//         .expect("failed to spawn glommio executor");
//
//     let tonic = rpc::DatabaseServer::new(DbServer {});
//     GmfServer::new(
//         hyper::service::service_fn({
//             move |req| {
//                 let mut tonic = tonic.clone();
//
//                 tonic.call(req)
//             }
//         }),
//         10240,
//     )
//     .serve(std::net::SocketAddr::from_str("0.0.0.0:50051").expect("invalid address"))
//     .map_err(|e| anyhow::anyhow!("Failed to run gmf server: {e}"))?;
//
//     let server = glommio::LocalExecutorBuilder::new(glommio::Placement::Unbound)
//         .name("server-executor")
//         .spawn(|| async move {
//             let executor = glommio::executor();
//
//             let queue = executor.create_task_queue(
//                 glommio::Shares::Static(10),
//                 glommio::Latency::Matters(Duration::from_millis(10)),
//                 "incoming-connections",
//             );
//
//             let listener = match glommio::net::UnixListener::bind("/tmp/mintdb.sock") {
//                 Ok(l) => l,
//                 Err(e) => {
//                     eprintln!("Failed to bind to socket: {}", e);
//
//                     return;
//                 }
//             };
//
//             loop {
//                 let stream = match listener.accept().await {
//                     Ok(s) => s,
//                     Err(e) => {
//                         eprintln!("Failed to accept connection: {}", e);
//                         continue;
//                     }
//                 };
//
//                 match executor.spawn_local_into(
//                     async move {
//                         // TODO: Handle the connection
//                         let _stream = stream;
//                     },
//                     queue,
//                 ) {
//                     Ok(task) => {
//                         task.detach();
//                     }
//                     Err(e) => {
//                         eprintln!("Failed to spawn task for connection: {}", e);
//                     }
//                 }
//             }
//         })
//         .expect("failed to spawn glommio executor");
//
//     server
//         .join()
//         .map_err(|e| anyhow::anyhow!("Failed to run server: {e}"))?;
//     database
//         .join()
//         .map_err(|e| anyhow::anyhow!("Failed to run database: {e}"))?;
//
//     Ok(())
// }
