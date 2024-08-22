
url_rabbit: str = "amqp://guest:guest@127.0.0.1/"
url_rabbitmq_aws: str = "amqps://xxhdmlgy:3VEb46SaBLY5eV7mVP_p0aKj7pMUUcy4@crow.rmq.cloudamqp.com/xxhdmlgy"
url_rabbit_google: str = "amqps://ilaieekx:2httl0idORgD1d_X4mc_nBYr7Hgxl3TZ@dog.lmq.cloudamqp.com/ilaieekx"
url_keydb: str = "redis://localhost"
url_redis: str = "redis://redis-12588.c226.eu-west-1-3.ec2.cloud.redislabs.com:18719"
url_redi: str = "redis-rediscloud-corrugated:18719"

redis_host: str = 'redis-12588.c226.eu-west-1-3.ec2.cloud.redislabs.com'
redis_port: int = 12588
redis_password: str = 'B7xPvO9hqt6Xpt2jfSxAa2PyWKOPEyW0'

# Postgresql 15  on AWS
db_user_aws: str = 'dyqbehwlykfjkc'
db_name_aws: str = 'd15bctjh2dhvtv'
db_password_aws: str = '8d3c812ab523bc9ea17bcbc6a5f20c4353a54db81479cf15a1fd083113a6bb1e'
db_port_aws: int = 5432
db_host_aws: str = 'ec2-54-195-24-35.eu-west-1.compute.amazonaws.com'

# Postgresql 15  on Local machine
user: str = 'postgres'
name: str = 'fintech'
password: str = 'aa4401'
port: int = 5432
host: str = 'localhost'

query_many: str = f"INSERT INTO message (start_date,status,id_distribution,id_client) VALUES ($1,$2,$3,$4);"

url_twp: str = "postgresql://gen_user:Bv(i%3D%26r5ILjTid@147.45.239.120:5432/default_db"
db_user_twp: str = "gen_user"
db_name_twp: str = "default_db"
db_password_twp: str = "Bv(i%3D%26r5ILjTid"
db_port_twp: int = 5432
db_host_twp: str = "147.45.239.120"
