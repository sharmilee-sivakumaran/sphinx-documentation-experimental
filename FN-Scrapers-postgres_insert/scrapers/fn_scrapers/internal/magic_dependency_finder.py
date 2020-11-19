from __future__ import absolute_import

import injector


def _find_bindings_for_class(cls):
    to_scan = [cls]
    seen = set()
    bindings = set()

    while to_scan:
        cls = to_scan.pop()

        if cls in seen:
            continue
        else:
            seen.add(cls)

        to_scan.extend(cls.__bases__)

        if hasattr(cls, '__init__') and hasattr(cls.__init__, '__bindings__'):
            if cls.__init__.__bindings__ == 'deferred':
                raise Exception("We don't support deferred bindings")
            bindings.update(x[0] for x in cls.__init__.__bindings__.values())

    return bindings


def _get_dependencies_from_provider(provider):
    if isinstance(provider, injector.ClassProvider):
        return _find_bindings_for_class(provider._cls)
    elif isinstance(provider, injector.CallableProvider):
        bindings = getattr(provider._callable, '__bindings__', [])
        if bindings == 'deferred':
            raise Exception("We don't support deferred bindings")
        return {x[0] for x in getattr(provider._callable, '__bindings__', {}).values()}
    elif isinstance(provider, injector.InstanceProvider):
        return set()
    else:
        raise Exception("Unknown provider")


def find_dependencies(inj, key):
    to_scan = [key]
    seen = set()
    dependencies = {key}

    while to_scan:
        key = to_scan.pop()

        if key in seen:
            continue
        else:
            seen.add(key)

        binding = inj.binder.get_binding(None, injector.BindingKey(key))
        provider = binding.provider
        if isinstance(provider, injector.ListOfProviders):
            new_deps = {d for p in provider._providers for d in _get_dependencies_from_provider(p)}
        else:
            new_deps = _get_dependencies_from_provider(provider)

        to_scan.extend(new_deps)
        dependencies.update(new_deps)

    return dependencies

