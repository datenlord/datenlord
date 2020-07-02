use etcd_rs::*;
// use tokio::prelude::*;

#[tokio::main]
async fn main() -> etcd_rs::Result<()> {
    let client = Client::connect(ClientConfig {
        endpoints: vec!["http://192.168.50.222:2379".to_owned()],
        auth: None,
        tls: None,
    })
    .await?;

    let key = "foo";
    let value = "bar";

    // Put a key-value pair
    let resp = client.kv().put(PutRequest::new(key, value)).await?;

    println!("Put Response: {:?}", resp);

    // Get the key-value pair
    let resp = client
        .kv()
        .range(RangeRequest::new(KeyRange::key(key)))
        .await?;
    println!("Range Response: {:?}", resp);

    // Delete the key-valeu pair
    let resp = client
        .kv()
        .delete(DeleteRequest::new(KeyRange::key(key)))
        .await?;
    println!("Delete Response: {:?}", resp);

    Ok(())
}
