use std::path;

use chrono::NaiveDateTime;
use futures::TryStreamExt;
use pulsar::{
    message::proto::command_subscribe::SubType,
    Consumer, Pulsar, TokioExecutor, Authentication
};


use structopt::StructOpt;


#[derive(StructOpt, Debug)]
#[structopt(name = "example", about = "An example of StructOpt usage.")]
struct Opt {
    #[structopt(long = "sourcepulsar", help = "Destination pulsar address", default_value = "pulsar://localhost:6650")]
    sourcepulsar: String,

    #[structopt(long = "sourcetopic", help = "Destination topic name", default_value = "non-persistent://public/functions/parsed")]
    sourcetopic: String,

    #[structopt(long = "sourcesubscription", help = "Destination producer name", default_value = "perf_aggregator")]
    sourcesubscription: String,

    #[structopt(long = "sourcetrustcerts", help = "Path for pem file, for ca.cert", default_value = "")]
    sourcetrustcerts: String,

    #[structopt(long = "sourcecertfile", help = "Path for cert.pem file", default_value = "")]
    sourcecertfile: String,

    #[structopt(long = "sourcekeyfile", help = "# path for key-pk8.pem file", default_value = "")]
    sourcekeyfile: String,

    #[structopt(long = "hostname_verification_enabled", help = "# hostname_verification_enabled")]
    hostname_verification_enabled: bool,

    #[structopt(long = "allow_insecure_connection", help = "# allow_insecure_connection")]
    allow_insecure_connection: bool
}

async fn pulsar_builder(pulsar: String, trustcerts: String, certfile: String, keyfile: String, allow_insecure_connection: bool, hostname_verification_enabled: bool) -> Result<Pulsar<TokioExecutor>, pulsar::Error> {

    if trustcerts.len() == 0 && certfile.len() == 0 && keyfile.len() == 0 {
        Pulsar::builder(pulsar, TokioExecutor)
            .build()
            .await
    } else {
        let auth = Authentication{
            name: "tls".to_string(),
            data: vec![],
        };

        Pulsar::builder(pulsar, TokioExecutor)
            .with_certificate_chain_file(path::Path::new(&trustcerts)).unwrap()
            //.with_identity_files(path::Path::new(&certfile), path::Path::new(&keyfile)).unwrap()
            .with_auth(auth)
            .with_allow_insecure_connection(allow_insecure_connection)
            .with_tls_hostname_verification_enabled(hostname_verification_enabled)
            .build()
            .await
    }

}


#[tokio::main]
async fn main() -> Result<(), pulsar::Error> {
    env_logger::builder()
        .format_timestamp_millis()
        .init();


    let opt = Opt::from_args();


    let pulsar: Pulsar<_> = pulsar_builder(opt.sourcepulsar, opt.sourcetrustcerts, opt.sourcecertfile, opt.sourcekeyfile, opt.allow_insecure_connection, opt.hostname_verification_enabled).await?;


    let mut consumer: Consumer<Vec<u8>, _> = pulsar
        .consumer()
        .with_topic(opt.sourcetopic)
        .with_consumer_name("test_consumer")
        .with_subscription_type(SubType::Exclusive)
        .with_subscription(opt.sourcesubscription)
        .with_batch_size(1000) //TODO batch size = 1 blocks...??
        .build()
        .await?;


    let mut counter = 0usize;
    while let Some(msg) = consumer.try_next().await? {
        consumer.ack(&msg).await?;
        counter += 1;
        // if counter%1_000 == 0 {
        if counter%10_000 == 0 {
            log::info!("got {counter} messages");
            match (msg.key(), std::str::from_utf8(&msg.payload.data)) {
                // (Some(key), Ok(_msg_str)) => log::debug!("{key} - {{msg_str}} {:?}", NaiveDateTime::from_timestamp_millis( msg.metadata().publish_time as i64).unwrap()),
                (Some(key), Ok(msg_str)) => log::debug!("{key} - {msg_str} {:?}", NaiveDateTime::from_timestamp_millis( msg.metadata().publish_time as i64).unwrap()),
                (None, Ok(_msg_str)) => log::debug!("No key - {{msg_str}}"),
                (Some(key), Err(_)) => log::debug!("{key} - Failed to read message"),
                (None, Err(_)) => log::debug!("Failed to read message"),
            }
        }
    }

    Ok(())
}
