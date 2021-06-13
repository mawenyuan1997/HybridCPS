import os

import utils
import time

if __name__ == "__main__":
    os.system('redis-server redis-stable/redis.conf &')
    os.system('python3 HybridCPS/Coordinator.py localhost 6000 &')
    n = 0
    config_dir = 'HybridCPS/SemiconductorMfg/RAconfig/'
    for config_file in os.listdir(config_dir):
        ra_name = config_file.split('.')[0]

        os.system('python3 HybridCPS/ResourceAgent.py {} {} {} {} &'.format(ra_name,
                                                                            'localhost',
                                                                            utils.PORT['RA start'] + n,
                                                                            config_dir + config_file))
        n += 1
    time.sleep(1)
    config_dir = 'HybridCPS/SemiconductorMfg/PAconfig/'
    os.system('python3 HybridCPS/ProductAgent.py {} {} {} {} &'.format("PA1",
                                                                           'localhost',
                                                                           utils.PORT['PA start'],
                                                                           config_dir + "PA1.json"))
