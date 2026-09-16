// rust-embed refuses to compile when the asset folder is missing. The web
// build populates it; for a bare `cargo test` an empty folder is enough.
fn main() {
    std::fs::create_dir_all("../../web/dist").expect("create web/dist");
    println!("cargo:rerun-if-changed=../../web/dist");
}
