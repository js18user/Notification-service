
url_rabbit: str = f"amqp://guest:guest@127.0.0.1/"
url_rabbitmq_aws: str = f"amqps://xxhdmlgy:3VEb46SaBLY5eV7mVP_p0aKj7pMUUcy4@crow.rmq.cloudamqp.com/xxhdmlgy"
url_rabbit_google: str = f"amqps://ilaieekx:2httl0idORgD1d_X4mc_nBYr7Hgxl3TZ@dog.lmq.cloudamqp.com/ilaieekx"
url_keydb: str = f"redis://localhost"
url_redis: str = f"redis://redis-12588.c226.eu-west-1-3.ec2.cloud.redislabs.com:18719"
url_redi: str = f"redis-rediscloud-corrugated:18719"

redis_host: str = f'redis-12588.c226.eu-west-1-3.ec2.cloud.redislabs.com'
redis_port: int = 12588
redis_password: str = f'B7xPvO9hqt6Xpt2jfSxAa2PyWKOPEyW0'

# Postgresql 15  on AWS
db_user_aws: str = f'dyqbehwlykfjkc'
db_name_aws: str = f'd15bctjh2dhvtv'
db_password_aws: str = f'8d3c812ab523bc9ea17bcbc6a5f20c4353a54db81479cf15a1fd083113a6bb1e'
db_port_aws: int = 5432
db_host_aws: str = f'ec2-54-195-24-35.eu-west-1.compute.amazonaws.com'

# Postgresql 15  on Local machine
user: str = f'postgres'
name: str = f'fintech'
password: str = f'aa4401'
port: int = 5432
host: str = f'localhost'

query_many: str = f"INSERT INTO message (start_date,status,id_distribution,id_client) VALUES ($1,$2,$3,$4);"

url_twp: str = f"postgresql://gen_user:Bv(i%3D%26r5ILjTid@147.45.239.120:5432/default_db"
db_user_twp: str = f"gen_user"
db_name_twp: str = f"default_db"
db_password_twp: str = f"Bv(i%3D%26r5ILjTid"
db_port_twp: int = 5432
db_host_twp: str = f"147.45.239.120"
