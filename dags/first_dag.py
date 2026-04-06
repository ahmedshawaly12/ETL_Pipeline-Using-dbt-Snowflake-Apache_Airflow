from airflow.sdk import dag, task


@dag(
    dag_id="first_dag"
)
def first_dag():

    @task.python
    def extract_data():
        print('Loading data layer')
        extracted_data = {'fist_name': 'ahmed', 'last_name': 'ali', 'age': 30}
        print("the data is loaded")
        return extracted_data
    
    @task.python
    def transform_data(data:dict):
        print('Transforming Layer')
        transformed_data = data
        return transformed_data
    
    @task.python
    def load_data(data:dict):
        print('Loading Layer')
        print ('loaded data is: ', data)
    
    first = extract_data()
    second = transform_data(first)
    third = load_data(second)



# Instantiating the dag
first_dag()


