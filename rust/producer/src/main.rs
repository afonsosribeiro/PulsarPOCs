mod simple_producer;
mod agg_producer;

use std::path;

use pulsar::{
    producer, Pulsar, TokioExecutor, proto, Authentication
};

use structopt::StructOpt;

#[derive(StructOpt, Debug)]
#[structopt(name = "example", about = "An example of StructOpt usage.")]
struct Opt {
    #[structopt(long = "destpulsar", help = "Destination pulsar address", default_value = "pulsar://localhost:6650")]
    destpulsar: String,

    #[structopt(long = "desttopic", help = "Destination topic name", default_value = "non-persistent://public/functions/agg")]
    desttopic: String,

    #[structopt(long = "destsubscription", help = "Destination producer name", default_value = "producer")]
    destsubscription: String,

    #[structopt(long = "desttrustcerts", help = "Path for pem file, for ca.cert", default_value = "")]
    desttrustcerts: String,

    #[structopt(long = "destcertfile", help = "Path for cert.pem file", default_value = "")]
    destcertfile: String,

    #[structopt(long = "destkeyfile", help = "# path for key-pk8.pem file", default_value = "")]
    destkeyfile: String,

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

        log::info!("--------------------------- {allow_insecure_connection} {hostname_verification_enabled}");

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


    let pulsar: Pulsar<_> = pulsar_builder(opt.destpulsar, opt.desttrustcerts, opt.destcertfile, opt.destkeyfile, opt.allow_insecure_connection, opt.hostname_verification_enabled).await?;

    let mut producer: pulsar::Producer<TokioExecutor> = pulsar
        .producer()
        .with_topic(opt.desttopic)
        .with_name(opt.destsubscription)
        .with_options(producer::ProducerOptions {
            batch_size: Some(200),
            batch_byte_size: Some(1024*1024),
            schema: Some(proto::Schema {
                r#type: proto::schema::Type::String as i32,
                ..Default::default()
            }),
            ..Default::default()
        })
        .build()
        .await?;

    producer
        .check_connection()
        .await
        .map(|_| log::info!("connection ok"))?;

    agg_producer::agg_producer(&mut producer).await
}
