class AttrsToStringMixin(object):
    def __repr__(self):
        data = ["{}={}".format(k, v) for k, v in self.__dict__.items()]
        return "{}({})".format(self.__class__.__name__, ", ".join(data))

    def __str__(self):
        return self.__repr__()
