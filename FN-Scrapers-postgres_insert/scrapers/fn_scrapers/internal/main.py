from __future__ import absolute_import

from .args import parse_args


def main():
    args = parse_args()

    from .config import set_config_from_fd, set_config_from_func, has_config
    from .unix_util import set_cloexec

    # Any locks we've been passed, mark as FD_CLOEXEC - we do this to
    # avoid passing them to any children we may spawn. Passing them
    # probably wouldn't do anything bad, but, there simply isn't a
    # good reason to.
    if args.lock_fd:
        for fd in args.lock_fd:
            fd = int(fd)
            set_cloexec(fd)

    CONFIGS = ["fn_rabbit.json", "config.yaml", "logging.yaml", "ratelimiter-config.json", "schedules.yaml"]

    if args.config_from_fd:
        # Register any configs that we've been passed. Doing so passes
        # ownership of the file descriptor to the config module - ie,
        # we don't need to worry about closing them.
        for config_spec in args.config_from_fd:
            config_fd, config_name = config_spec.split(":", 2)
            config_fd = int(config_fd)
            set_cloexec(config_fd)
            set_config_from_fd(config_name, config_fd)
        for config_name in CONFIGS:
            if not has_config(config_name):
                raise Exception(u"Require configuration for '{}' not passed".format(config_name))
    else:
        # If we weren't passed in any configs, we register those configs
        # ourselves to be read from the filesystem in the current working
        # directory.
        def _read_config(config_name):
            if config_name == "fn_rabbit.json":
                config_name = ".fn_rabbit.json"

            with open(config_name) as f:
                return f.read()

        for config_name in CONFIGS:
            if not has_config(config_name):
                set_config_from_func(config_name, _read_config)

    import importlib
    command_module_name, command_func_name = args.command
    command_module = importlib.import_module(command_module_name)
    getattr(command_module, command_func_name)(args)
