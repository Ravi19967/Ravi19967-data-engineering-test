import abc


class ETLInterface(metaclass=abc.ABCMeta):
    """
    Interface for any ETL Pipeline. It creates 3 abstract methods.
    read_data -> Extracts data from the source
    transform_data -> Transforms the read data
    write_data -> Writes transformed data to the destination
    """

    @classmethod
    def __subclasshook__(cls, subclass):
        return (
            hasattr(subclass, "read_data")
            and callable(subclass.read_data)
            and hasattr(subclass, "transform_data")
            and callable(subclass.transform_data)
            and hasattr(subclass, "write_data")
            and callable(subclass.write_data)
            or NotImplemented
        )

    @abc.abstractmethod
    def read_data(self):
        """Load in the dataset"""
        raise NotImplementedError

    @abc.abstractmethod
    def transform_data(self):
        """Transform the dataset"""
        raise NotImplementedError

    @abc.abstractmethod
    def write_data(self):
        """Wrtie the output dataset"""
        raise NotImplementedError
