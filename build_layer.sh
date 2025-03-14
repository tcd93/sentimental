rm -rf temp_layer
mkdir -p temp_layer/python

# Install packages into the temporary directory
# use --platform manylinux2014_x86_64 to ensure compatibility with AWS Lambda
pip install -r ./layers/requirements.txt -t temp_layer/python --only-binary=:all: --upgrade --no-cache-dir\
 --platform manylinux2014_x86_64

cd temp_layer
zip -r ../layers/python-api-packages.zip .

# Clean up
rm -rf temp_layer
