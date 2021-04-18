import os
import random
import time
from collections import deque
from datetime import datetime
from pathlib import Path

import numpy as np
import wandb
from keras.layers import Dense
from keras.models import Sequential
from keras.optimizers import Adam
from wandb.keras import WandbCallback


# Decorator to time a function and log the
# time it took to WandB
#
def wandb_timing(func):
    def wrapper(*args, **kwargs):
        t1 = time.time()
        res = func(*args, **kwargs)
        t2 = time.time()

        wandb_dict = {}
        wandb_dict["time_{}".format(func.__name__)] = t2 - t1
        wandb.log(wandb_dict, commit=False)

        return res

    return wrapper


# ReplayBuffer class to handle collection and
# storage of experiences from the gym
#
class ReplayBuffer:
    def __init__(self, size, broken=False):
        self.buffer_size = size
        self.count = 0
        self.buffer = deque(maxlen=size)
        self.broken = broken

    def full(self):
        return self.count == self.buffer_size

    def add(self, xp):
        if self.count < self.buffer_size:
            self.buffer.append(xp)
            self.count += 1
        else:
            if self.broken:
                self.buffer[0] = xp
            else:
                self.buffer.popleft()
                self.buffer.append(xp)

    def size(self):
        return self.count

    def sample(self, num_samples):
        return random.sample(self.buffer, min(self.count, num_samples))

    def clear(self):
        self.buffer.clear()
        self.count = 0


class DQNDefault:
    def __init__(self, env, config):
        self.global_log_path = config['global']['log-path']
        self.local_log_path = self.global_log_path + '/rl-agent'
        config['global']['context']['rl-agent'] = {}
        self.system_context = config['global']['context']
        self.rl_context = self.system_context['rl-agent']
        self.logger = config['global']['logger']
        os.makedirs(self.local_log_path)
        self.name = self.identifier()
        self.config = config
        self.evaluation_history = {}

        self.buffer = ReplayBuffer(config['replay-buffer-size'], broken=config['broken-buffer'])
        self.callbacks = None
        self.epochs_run = 0
        self.episodes_run = 0
        self.last_rewards = list()
        self.max_average_reward = 0
        self.environment_won = False

        self.learning_rate = config['learning-rate']
        self.batch_size = config['batch-size']
        self.gamma = config['gamma']  # horizon
        self.tau = config['tau']  # how much the target weights are updated
        self.synchronize_networks_every = config['synchronize_networks_every']
        self.sync_eps_run = 1

        # Initially: completely random exploration of state spaces
        self.epsilon = config['epsilon-initial']
        self.epsilon_min = config['epsilon-min']
        self.epsilon_decay = config['epsilon-decay']

        wandb.init(entity=self.config['wandb-team-name'], project=self.config['wandb-project-name'],
                   group=self.config['wandb-group-name'], monitor_gym=True,
                   name=self.name)
        wandb.config.update(self.config)

        self.env = env

        self.model = self.create_model()
        self.target_model = self.create_model()
        # Ensure both models have the same weights
        self.hard_update_target_network()

        print(self.name, "setup complete.")

    def identifier(self):
        return datetime.now().strftime(self.__class__.__name__ + "_%Y%m%d-%H%M%S")

    def train(self):
        self.rl_context['isTrain'] = True
        self.logger.select_log('train')
        for episode in range(self.config['episodes']):
            self.logger.new_record()
            self.logger.log('episode', episode)
            self.rl_context['current_episode'] = episode
            running_reward_sum = 0
            if episode % 5 == 0:
                self.evaluate_at_episode(episode)
                self.logger.select_log('train')

            state = self.env.reset()
            done = False
            current_step = 0
            experience = []

            # print("New Episode")
            while not done:
                action = self.choose_action(self.reshape_state(state))
                new_state, reward, done, _info = self.step_action(action)

                state_tmp = self.reshape_state(state.copy())
                new_state_tmp = self.reshape_state(new_state.copy())

                experience.append([state_tmp, action, 0, new_state_tmp, done])

                state = new_state.copy()

                if done:
                    for idx, exp in enumerate(experience):
                        cost = reward[idx]['cost']
                        exp[2] = cost
                        self.write_to_buffer(exp)

                    if self.buffer.size() > self.config['min-experiences-to-train']:
                        self.replay()
                        if self.sync_eps_run >= self.synchronize_networks_every:  # Freeze prediction network for some episodes
                            self.update_target_network()
                            self.sync_eps_run = 0
                        self.sync_eps_run += 1

                    self.end_episode(current_step, running_reward_sum)
                    break

                current_step += 1
        self.save_models()
        self.env.close()

    def evaluate_at_episode(self, episode):
        self.logger.select_log('train-eval')
        self.evaluation_history[episode] = {}
        for idx, query in enumerate(self.env.query_generator.get_test_set()):
            self.logger.new_record()
            self.logger.log('episode', episode)
            self.logger.log('eval-query-nr', idx)
            self.logger.log('logical-query', str(query.copy()))
            state = self.env.reset_with_query(query)
            state = state.reshape((1, self.env.observation_space.shape[0]))
            done = False
            while not done:
                possible_steps = self.env.possible_steps()
                state = state.reshape((1, self.env.observation_space.shape[0]))
                prediction = self.model.predict(state)[0]
                for idx, action in enumerate(possible_steps):
                    if action == 0:
                        prediction[idx] = -np.inf
                action = np.argmax(prediction)
                state, reward, done, _info = self.env.step(action)

            join_order = self.env.join_order
            self.evaluation_history[episode][hash(tuple(query))] = join_order

    def reshape_state(self, state):
        return state.reshape((1, self.env.observation_space.shape[0]))

    def create_model(self):
        model = Sequential()

        layers = self.config['layers']
        for layer_nr in layers:
            if layer_nr == 1:
                model.add(
                    Dense(layers[layer_nr]['units'], input_dim=self.env.observation_space.shape[0],
                          activation=layers[layer_nr]['activation']))
            else:
                model.add(Dense(layers[layer_nr]['units'], activation=layers[layer_nr]['activation']))

        # Output Layer
        if self.config['environment-continuous']:
            model.add(Dense(self.env.action_space.shape[0], activation=self.config['output-activation']))
        else:
            model.add(Dense(self.env.action_space.n, activation=self.config['output-activation']))

        model.compile(loss=wandb.config.loss, optimizer=Adam(lr=self.learning_rate))

        return model

    # @wandb_timing
    def write_to_buffer(self, arr):
        self.buffer.add(arr)

    def save_models(self, suffix=''):
        Path(self.local_log_path).mkdir(exist_ok=True, parents=True)
        self.model.save(self.local_log_path + '/model.h5')
        self.target_model.save(self.local_log_path + '/target-model.h5')

    def get_callbacks(self):
        if self.callbacks is None:
            self.callbacks = [
                WandbCallback(save_model=False)
            ]

        return self.callbacks

    # @wandb_timing
    def replay(self):
        # Load :batch_size samples from the buffer
        # Experiences have the format: [state, action, reward, new_state, done]
        #
        mini_batch = np.asarray(self.buffer.sample(self.batch_size))

        # Convert the mini batch to usable numpy arrays
        states = np.stack(mini_batch[:, 0], axis=0)[:, 0, :]
        actions = mini_batch[:, 1].astype(np.int)
        rewards = mini_batch[:, 2]
        new_states = np.stack(mini_batch[:, 3], axis=0)[:, 0, :]
        inverted_dones = np.invert(mini_batch[:, 4].astype(np.bool))

        # Predict the Q-value for the given states as well as the new states (for the adjustment)
        targets = self.model.predict(states)
        Q_new_states = self.target_model.predict(new_states)

        # Set reward for all used actions (not-dones handled below)
        # ==========================================================
        #
        targets[np.arange(targets.shape[0]), actions] = rewards

        # Adjust the states that were NOT done using
        # reward = reward + gamma * Q(new_state), where the
        # base reward is already applied (see above)
        # ==========================================================
        #
        not_done_actions = mini_batch[inverted_dones, 1].astype(np.int)

        # adjust reward for "not-done" states
        targets[inverted_dones, not_done_actions] += \
            self.gamma * Q_new_states[inverted_dones, np.argmax(Q_new_states[inverted_dones, :], axis=1)]

        # print(states)
        # print(targets)

        self.model.fit(states, targets, epochs=self.epochs_run + 1, callbacks=self.get_callbacks(), verbose=0,
                       initial_epoch=self.epochs_run)
        self.epochs_run += 1

    def hard_update_target_network(self):
        weights = self.model.get_weights()
        self.target_model.set_weights(weights)

    # @wandb_timing
    def update_target_network(self):
        weights = self.model.get_weights()
        target_weights = self.target_model.get_weights()

        for i in range(len(target_weights)):
            target_weights[i] = weights[i] * self.tau + target_weights[i] * (1 - self.tau)

        self.target_model.set_weights(target_weights)

    # @wandb_timing
    def step_action(self, action):
        return self.env.step(action)

    # @wandb_timing
    def choose_action(self, state):
        if random.random() < self.epsilon:
            # Choose random action
            possible_steps = self.env.possible_steps()
            possible_steps = [i for i, x in enumerate(possible_steps) if x]
            action = np.random.choice(possible_steps)
            return action

        else:
            possible_steps = self.env.possible_steps()
            prediction = self.model.predict(state)[0]
            for idx, action in enumerate(possible_steps):
                if action == 0:
                    prediction[idx] = -np.inf

            return np.argmax(prediction)

    def end_episode(self, total_steps, end_reward):
        self.update_running_rewards(end_reward)

        self.episodes_run += 1
        self.epsilon = max(self.epsilon_min, self.epsilon * self.epsilon_decay)
        wandb.log({
            'epsilon': self.epsilon,
            'buffer_size': self.buffer.size(),
            'current_episode': self.episodes_run - 1,
            'steps_til_end': total_steps
        }, commit=False)

    def update_running_rewards(self, reward_sum):
        self.last_rewards.append(reward_sum)

        if len(self.last_rewards) > 100:
            self.last_rewards = self.last_rewards[1:]

        new_average_reward = np.mean(self.last_rewards)

        wandb.log({
            '100_episode_rewards': new_average_reward,
            'end_reward': reward_sum,
            'max_avg_reward': self.max_average_reward
        }, commit=False)

        # Update average reward maximum
        if new_average_reward > self.max_average_reward:
            self.max_average_reward = new_average_reward
