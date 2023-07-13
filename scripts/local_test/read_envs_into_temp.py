import yaml

file = open(".github/workflows/cron.yml", 'r')
cron_config = yaml.load(file.read(),Loader=yaml.FullLoader)
file.close

env_map=cron_config["env"]

output=""
for key in env_map:
    output+=key+"="+str(env_map[key])+"\n"


f = open('read_envs_temp',mode='a+')
f.write(output)
f.close()
