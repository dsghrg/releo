from rl_algorithms.dqn_default import DQNDefault


def get_rl_agent(name, env, cfg):
    if name == 'dqn-default':
        return DQNDefault(env, cfg)
