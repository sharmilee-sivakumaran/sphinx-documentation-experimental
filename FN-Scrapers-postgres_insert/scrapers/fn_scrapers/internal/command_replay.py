from __future__ import absolute_import


class ProxyObject(object):
    def __init__(self, real_object, command_replay):
        self.__real_object = real_object
        self.__command_replay = command_replay

    def __getattr__(self, attr):
        def fn(*args, **kwargs):
            result = getattr(self.__real_object, attr)(*args, **kwargs)
            proxy_result = ProxyObject(result, self.__command_replay)

            self.__command_replay._commands.append([self.__command_replay._results[id(self)], attr, args, kwargs])

            self.__command_replay._all_results.append(proxy_result)
            self.__command_replay._results[id(proxy_result)] = self.__command_replay._next_result_id
            self.__command_replay._next_result_id += 1

            return proxy_result

        return fn


class CommandReplay(object):
    def __init__(self, root_object):
        self._root_proxy = ProxyObject(root_object, self)

        self._all_results = []  # Keep results alive so that id()s won't be re-used

        self._results = {id(self._root_proxy): 1}  # map id(object) -> object_id

        self._next_result_id = 2

        # A list of commands. Each command it itself a list of 5 elements:
        # "object_id", "function", "args", "kwargs"
        self._commands = []

    @property
    def proxy(self):
        return self._root_proxy

    @property
    def commands(self):
        return self._commands


def replay(commands, root_object):
    results = {1: root_object}
    for idx, (object_id, function, args, kwargs) in enumerate(commands):
        obj = results[object_id]
        result = getattr(obj, function)(*args, **kwargs)
        results[idx + 2] = result
