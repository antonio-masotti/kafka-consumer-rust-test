use std::time::Duration;

#[tokio::main]
pub async fn send_request(payload: String) {
    let http_client = reqwest::Client::new();
    let url = String::from("https://echo.zuplo.io");
    let res = http_client
        .post(&url)
        .body(payload)
        .header("Content-Type", "application/json")
        .timeout(Duration::from_secs(2))
        .send()
        .await;

    match res {
        Ok(response) => {
            println!("Response: {}", response.status());
            println!("Response: {}", response.text().await.unwrap());
        }
        Err(e) => {
            println!("Error: {}", e);
        }
    }
}
