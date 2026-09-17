fn main() -> Result<(), Box<dyn std::error::Error>> {
    tonic_prost_build::compile_protos("proto/order.proto")?;
    tonic_prost_build::compile_protos("proto/payment.proto")?;
    Ok(())
}
