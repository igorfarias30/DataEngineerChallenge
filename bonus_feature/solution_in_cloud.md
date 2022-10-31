# How you would set up the application using any cloud provider

As I am containerized the role application I can use Kubernetes for managing the containers. With this, I can
scale the application horizontally. For example, if a have lot of data to process, I can increase the number of workers to scale the Airflow cluster. If I am using AWS, I can increase the number of EC2 instances that will host my workers.

For another hand, when the number of request increase: I can scale out the number of instance of my Rest API app to maintain a response time
lower as possible and avoid timeouts.

I would setup that approach in a pipeline and every update a job will triggered to generate a new version of my cluster.
