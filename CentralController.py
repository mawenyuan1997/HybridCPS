def optimize():
    current_env = {}
    contain_all = False
    while not contain_all:
        current_env = self.knowledge.copy()
        contain_all = True
        for d in self.interests:
            if not (d in current_env and len(current_env[d]) == 4):
                contain_all = False
                break

    print('current env: {}'.format(current_env))
    start_time = time.time()

    def dist(a, b):
        return abs(a[0] - b[0]) + abs(a[1] - b[1])

    opt_A, opt_B = None, None
    quickest = 100000
    for ra_A in current_env['capability'].keys():
        if 'A' in current_env['capability'][ra_A]:
            for ra_B in current_env['capability'].keys():
                if 'B' in current_env['capability'][ra_B]:
                    pos_A = current_env['position'][ra_A]
                    pos_B = current_env['position'][ra_B]
                    duration = dist(self.current_pos, pos_A) + current_env['capability'][ra_A]['A'] + dist(pos_A,
                                                                                                           pos_B) + \
                               current_env['capability'][ra_B]['B']
                    if duration < quickest:
                        quickest = duration
                        opt_A, opt_B = ra_A, ra_B