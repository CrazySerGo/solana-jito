use allnodes_client::get_bootstrap_info;
use std::env;

#[tokio::test]
async fn test_get_bootstrap_info_with_env_var() {
    // Set the environment variable to a mock endpoint
    env::set_var("SOLANA_BOOTSTRAP_ENDPOINTS", "http://127.0.0.1:8899");

    // Call the function that uses the environment variable
    let (bootstrap_snapshot_node, voting_patch_flags) = get_bootstrap_info(0).await;

    // Since we're not running a real server, we expect the result to be None
    assert!(bootstrap_snapshot_node.is_none());
    assert!(voting_patch_flags.is_none());

    // Unset the environment variable to avoid affecting other tests
    env::remove_var("SOLANA_BOOTSTRAP_ENDPOINTS");
}
