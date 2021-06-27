import os

import utils
import time

if __name__ == "__main__":

    os.system('python3 CentralController.py {} {} &'.format(utils.IP['central controller'],
                                                            utils.PORT['central controller']))
    n = 0
    config_dir = 'SemiconductorMfg/RAconfig/'
    for config_file in sorted(os.listdir(config_dir)):
        ra_name = config_file.split('.')[0]
        print(ra_name, utils.PORT['RA start'] + n)
        os.system('python3 ResourceAgent.py {} {} {} {} {} &'.format(ra_name,
                                                                     '127.0.0.1',
                                                                     utils.PORT['RA start'] + n,
                                                                     config_dir + config_file,
                                                                     'centralized'))
        n += 1
    os.system('python3 Coordinator.py {} {} &'.format(utils.IP['coordinator'], utils.PORT['coordinator']))
    time.sleep(12)
    config_dir = 'SemiconductorMfg/PAconfig/'
    # os.system('python3 ProductAgent.py {} {} {} {} {} &'.format("PA1",
    #                                                             '127.0.0.1',
    #                                                             utils.PORT['PA start'],
    #                                                             config_dir + "PA1.json",
    #                                                             'centralized'))
