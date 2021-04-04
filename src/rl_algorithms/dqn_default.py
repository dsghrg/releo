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

BASE_CONFIG = {
    'environment_name': 'RL_joinorder',
    'environment_continuous': False,
    'environment_death_reward_requirement': -np.inf,
    'num_episodes': 300,
    'epsilon_initial': 1.0,
    'epsilon_min': 0.03,
    'epsilon_decay': 0.9925,  # how quickly do we change over to predictions
    'gamma': 0.9,  # horizon: how far are q-value updates propagated
    'tau': 0.1,  # how much are the target weights updated
    'batch_size': 8,
    'learning_rate': 0.003,
    'n_dense1': 32,
    'n_dense2': 64,
    'n_dense3': 16,
    'output_activation': 'linear',  # softmax
    'loss': 'huber_loss',  # huber_loss categorical_crossentropy mean_squared_error
    'optimizer': 'adam',
    'replay_buffer_size': 1024,
    'min_experiences_to_train': 32,
    'checkpoint_period': 20,
    'wandb_group_name': 'debug',
    'wandb_project_name': 'rl-joinorder-luc',
    'broken_buffer': False
}


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
    def __init__(self, env, config=BASE_CONFIG):
        self.name = self.identifier()
        self.config = config

        self.buffer = ReplayBuffer(config['replay_buffer_size'], broken=config['broken_buffer'])
        self.callbacks = None
        self.epochs_run = 0
        self.episodes_run = 0
        self.last_rewards = list()
        self.max_average_reward = 0.5 * config['environment_death_reward_requirement']
        self.environment_won = False

        self.learning_rate = config['learning_rate']
        self.batch_size = config['batch_size']
        self.gamma = config['gamma']  # horizon
        self.tau = config['tau']  # how much the target weights are updated

        # Initially: completely random exploration of state spaces
        self.epsilon = config['epsilon_initial']
        self.epsilon_min = config['epsilon_min']
        self.epsilon_decay = config['epsilon_decay']

        wandb.init(project=self.config['wandb_project_name'], group=self.config['wandb_group_name'], monitor_gym=True,
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
        for episode in range(self.config['num_episodes']):
            running_reward_sum = 0
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

                    if self.buffer.size() > self.config['min_experiences_to_train']:
                        self.update_target_network()
                        self.replay()

                    if self.episodes_run % self.config['checkpoint_period'] == 0:
                        self.save_model()

                    self.end_episode(current_step, running_reward_sum)
                    break

                current_step += 1

            # Abort the run if the reward gets too low
            # if len(self.last_rewards) > 32:
            #     if np.mean(self.last_rewards) < self.config['environment_death_reward_requirement']:
            #         print("ABORTED!", np.mean(self.last_rewards), self.config['environment_death_reward_requirement'])
            #         print(self.last_rewards)
            #         break

        self.env.close()

    def reshape_state(self, state):
        return state.reshape((1, self.env.observation_space.shape[0]))

    def create_model(self):
        model = Sequential()

        model.add(Dense(wandb.config.n_dense1, input_dim=self.env.observation_space.shape[0], activation="relu"))

        if wandb.config.n_dense2 > 0:
            model.add(Dense(wandb.config.n_dense2, activation="relu"))

        if wandb.config.n_dense3 > 0:
            model.add(Dense(wandb.config.n_dense3, activation="relu"))

        # Output Layer
        if self.config['environment_continuous']:
            model.add(Dense(self.env.action_space.shape[0], activation=self.config['output_activation']))
        else:
            model.add(Dense(self.env.action_space.n, activation=self.config['output_activation']))

        model.compile(loss=wandb.config.loss, optimizer=Adam(lr=self.learning_rate))

        # model.summary()
        return model

    # @wandb_timing
    def write_to_buffer(self, arr):
        self.buffer.add(arr)

    def reset(self):
        wandb.init(project=self.config['wandb_project_name'], group=self.config['wandb_group_name'], entity='releo',
                   monitor_gym=True, name=self.name)
        wandb.config.update(self.config)

        env = RL_joinorder()

        if self.buffer.size() > 0:
            self.buffer.clear()

        self.epsilon = self.config['epsilon_initial']
        self.epochs_run = 0
        self.episodes_run = 0
        self.last_rewards = list()
        self.max_average_reward = 0.5 * self.config['environment_death_reward_requirement']
        self.environment_won = False
        self.model = self.create_model()
        self.target_model = self.create_model()
        # Ensure both models have the same weights
        self.hard_update_target_network()

        return self.env.reset()

    def save_model(self, suffix=''):
        Path('./RL_program/models/' + self.name).mkdir(exist_ok=True, parents=True)
        self.model.save(
            './RL_program/models/' + self.name + '/' + self.name + '_ep' + str(self.episodes_run) + suffix + '.h5')

    def save_best_model(self):
        Path('./RL_program/models/' + self.name).mkdir(exist_ok=True, parents=True)
        self.model.save('./RL_program/models/' + self.name + '/' + self.name + '_best.h5')

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

            if self.config['environment_continuous']:
                return prediction
            else:
                # env.possible_steps()
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
            self.save_best_model()
