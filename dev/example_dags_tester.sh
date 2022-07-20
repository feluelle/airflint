# Update the repo or clone it if it doesn't exist
cd /tmp/airflow && git fetch || gh repo clone apache/airflow /tmp/airflow && cd /tmp/airflow
example_dags_dir=/tmp/airflow/airflow/example_dags/
# We go through all Airflow 2 tags
for tag in $(git tag -l "2\.[0-9]*\.[0-9]*" | grep -vE "rc|b|a[0-9]*"); do
    echo "Running for tag $tag"
    git checkout $tag > /dev/null 2>&1
    pip install apache-airflow==$tag > /dev/null 2>&1
    time for file in $example_dags_dir*; do python "$file" > /dev/null 2>&1; done || true
    airflint -a $example_dags_dir
    autoflake --remove-all-unused-imports -r -i $example_dags_dir
    time for file in $example_dags_dir*; do python "$file" > /dev/null 2>&1; done || true
    git restore .
done
