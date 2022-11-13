# rest_api_bcrm_with_nginx

added nginx 
Web Server Gateway Interface

![image](https://user-images.githubusercontent.com/27881527/201516107-f2594e4b-f154-4eea-a3dc-270a632503fb.png)

Figure 1 shows the Architecture for REST API. in this instead of exposing the REST API directly to the client we are using ngnix server which serves the requests at port 80. we are using wsgi which will create multiple instance of your Flask API and host it on different Port.it will create instance of your API on Port X ... Port Y

load balancing will make sure to route the traffic to the necessary Port based on round robin algorithms
