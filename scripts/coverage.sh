#/bin/bash
set -u -e

rm -rf coverage
mkdir coverage

if [[ ! -v CI ]]; then
  FORMAT=html
  OUTPUT_DATENLORD=coverage/datenlord_html
else
  FORMAT=lcov
  OUTPUT_DATENLORD=coverage/datenlord_cov.lcovrc
fi

# generate coverage data
CARGO_INCREMENTAL=0 RUSTFLAGS='-Cinstrument-coverage' LLVM_PROFILE_FILE='coverage-%p-%m.profraw' cargo test --lib

# generate report
echo "generating datenlord coverage..."
grcov . \
  --binary-path ./target/debug/ \
  --source-dir ./src \
  -t $FORMAT \
  --branch \
  --ignore-not-existing \
  --excl-br-start '^(pub(\((crate|super)\))? )?mod tests' \
  --excl-br-stop '^}' \
  --ignore="*/tests/*" \
  -o $OUTPUT_DATENLORD

# cleanup
echo "cleaning up..."
find . -type f -name '*.profraw' -delete