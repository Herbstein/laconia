fn main() -> Result<(), Box<dyn std::error::Error>> {
    let mut builder = tonic_build::configure();

    if cfg!(feature = "client") {
        builder = builder.build_client(true);
    } else {
        builder = builder.build_client(false);
    }

    if cfg!(feature = "server") {
        builder = builder.build_server(true);
    } else {
        builder = builder.build_server(false);
    }

    builder.compile_protos(&["proto/liveness.proto"], &["proto"])?;

    Ok(())
}
