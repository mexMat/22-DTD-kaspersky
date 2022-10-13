import subprocess
import os
from multiprocessing import Pool


class Assembly:
    def __init__(self, config):
        self.goals = config['goals']
        self.jobs = config['jobs']
        self.tree = [self.make_tree(BTree(), x, cur_dir="artifacts/") for x in config['goals']]

    def make_report(self, status):
        result_dict = {'state': "success"}
        job_list = []
        for x in self.jobs:
            if x['name'] not in status:
                result_dict['state'] = "failure"
            else:
                if status[x['name']] != "success":
                    result_dict['state'] = "failure"
        for x in status:
            job_dict = {'name': x, "state": status[x]}
            if x in self.goals and result_dict['state'] != "failure":
                for y in self.tree:
                    if y.elem.name == x:
                        job_dict['artifact'] = os.path.join(y.elem.parent_dir,
                                                            y.elem.job_dir)
            job_list.append(job_dict)
        result_dict['jobs'] = job_list
        return result_dict

    def execute(self):
        try:
            status = self.do_target()
        except RuntimeError as e:
            (status,) = e.args
        return self.make_report(status)

    def do_target(self):
        def do_pool(pool, status, pool_task):
            multiple_process = [pool.apply_async(x.do, ()) for x in pool_task]
            for process in multiple_process:
                process.wait()
            for y in [x.get() for x in multiple_process]:
                status[y[0]] = y[1]
                if y[1] != "success":
                    raise RuntimeError(status)
        proc_status = dict()
        for x in self.tree:
            sequence = x.reverse_tree()
            proc_pool_task = []
            with Pool(processes=3) as proc_pool:
                while sequence:
                    proc_status[sequence[-1].name] = "failure"
                    if not sequence[-1].has_depend(proc_status) and len(proc_pool_task) < 3:
                        proc_pool_task.append(sequence.pop())
                    else:
                        do_pool(proc_pool, proc_status, proc_pool_task)
                        proc_pool_task = []
                do_pool(proc_pool, proc_status, proc_pool_task)
        return proc_status

    def make_tree(self, tree, goal, cur_dir):
        for x in self.jobs:
            if x['name'] == goal:
                tree.elem = Job(x, job_dir=f"{cur_dir}{goal}/")
                if tree.elem.depends_on is not None:
                    for y in tree.elem.depends_on:
                        tree.depend.append(self.make_tree(BTree(), y, cur_dir=f"{cur_dir}{goal}/input/"))
                return tree

    def __repr__(self):
        result = "goals:\n"
        for x in self.goals:
            result += f"  {x}\n"
        result += "jobs:\n"
        for x in self.jobs:
            result += f"  {x['name']}\n"
        return result

    def __str__(self):
        return self.__repr__()


class Job:
    def __init__(self, job_config, job_dir="./"):
        self.name = job_config['name']
        self.commands = " && ".join(job_config['commands']) if 'commands' in job_config else None
        self.parent_dir = os.getcwd()
        self.job_dir = job_dir
        self.depends_on = job_config['depends_on'] if 'depends_on' in job_config else None
        self.timeout = job_config['timeout'] if 'timeout' in job_config else None

    def has_depend(self, status):
        result = False
        if self.depends_on is not None:
            for x in self.depends_on:
                if x not in status:
                    result = True
                else:
                    if status[x] == "failure":
                        result = True
        return result

    def do(self):
        directory = os.path.join(self.parent_dir, self.job_dir)
        if not os.path.exists(directory):
            os.makedirs(directory)
        os.chdir(directory)
        is_done = "success"
        try:
            subprocess.run(self.commands, timeout=self.timeout, check=True, shell=True)
        except subprocess.TimeoutExpired:
            is_done = "timeout"
        except subprocess.CalledProcessError:
            is_done = "failure"
        return [self.name, is_done]

    def __repr__(self):
        result = f"name: {self.name}\n"
        result += f"  commands: {self.commands}\n"
        result += f"  depend: {self.depends_on}\n"
        result += f"  timeout: {self.timeout}\n"
        result += f"  dir: {self.job_dir}\n"
        return result

    def __str__(self):
        return self.__repr__()


class BTree:
    def __init__(self, elem=None):
        self.depend = []
        self.elem = elem

    def reverse_tree(self, result=[]):
        if not self.depend:
            return [self.elem]
        else:
            result.extend([self.elem])
            for x in self.depend:
                result.extend(x.reverse_tree(result=[]))
        return result

    def __str__(self):
        result = self.elem.__str__()
        for x in self.depend:
            result += x.__str__()
        return result
