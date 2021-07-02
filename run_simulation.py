import os

import utils
from utils import send_msg
import time

if __name__ == "__main__":

    os.system('python3 scheduler.py {} {} &'.format(utils.IP['scheduler'],
                                                            utils.PORT['scheduler']))
    os.system('python3 coordinator.py {} {} &'.format(utils.IP['coordinator'], utils.PORT['coordinator']))
    os.system('python3 monitor.py {} {} &'.format(utils.IP['monitor'], utils.PORT['monitor']))
    time.sleep(3)
    config_dir = 'SemiconductorMfg/RAconfig/'
    for config_file in sorted(os.listdir(config_dir)):
        ra_name = config_file.split('.')[0]
        send_msg((utils.IP['coordinator'], utils.PORT['coordinator']), {'type': 'new resource',
                                                                        'RA name': ra_name,
                                                                        'config file': config_dir + config_file})

    time.sleep(16)
    config_dir = 'SemiconductorMfg/PAconfig/'
    send_msg((utils.IP['coordinator'], utils.PORT['coordinator']), {'type': 'new product',
                                                                    'PA name': "1_Part1",
                                                                    'config file': config_dir + "PA1.json"})
