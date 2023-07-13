source scripts/local_test/load_envs.sh


docker build .\
    --build-arg RUST_IMAGE_VERSION=1.61.0\
    --file ./scripts/local_test/4_redeploy_after_build/Dockerfile\
    --target datenlord\
    --tag datenlord/datenlord:e2e_test