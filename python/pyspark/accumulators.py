#
# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

"""
>>> from pyspark.context import SparkContext
>>> sc = SparkContext('local', 'test')
>>> a = sc.accumulator(1)
>>> a.value
1
>>> a.value = 2
>>> a.value
2
>>> a += 5
>>> a.value
7

>>> sc.accumulator(1.0).value
1.0

>>> sc.accumulator(1j).value
1j

>>> rdd = sc.parallelize([1,2,3])
>>> def f(x):
...     global a
...     a += x
>>> rdd.foreach(f)
>>> a.value
13

>>> b = sc.accumulator(0)
>>> def g(x):
...     b.add(x)
>>> rdd.foreach(g)
>>> b.value
6

>>> from pyspark.accumulators import AccumulatorParam
>>> class VectorAccumulatorParam(AccumulatorParam):
...     def zero(self, value):
...         return [0.0] * len(value)
...     def addInPlace(self, val1, val2):
...         for i in range(len(val1)):
...              val1[i] += val2[i]
...         return val1
>>> va = sc.accumulator([1.0, 2.0, 3.0], VectorAccumulatorParam())
>>> va.value
[1.0, 2.0, 3.0]
>>> def g(x):
...     global va
...     va += [x] * 3
>>> rdd.foreach(g)
>>> va.value
[7.0, 8.0, 9.0]

>>> rdd.map(lambda x: a.value).collect() # doctest: +IGNORE_EXCEPTION_DETAIL
Traceback (most recent call last):
    ...
Py4JJavaError:...

>>> def h(x):
...     global a
...     a.value = 7
>>> rdd.foreach(h) # doctest: +IGNORE_EXCEPTION_DETAIL
Traceback (most recent call last):
    ...
Py4JJavaError:...

>>> sc.accumulator([1.0, 2.0, 3.0]) # doctest: +IGNORE_EXCEPTION_DETAIL
Traceback (most recent call last):
    ...
TypeError:...
"""

import sys
import select
import struct
if sys.version < '3':
    import SocketServer
else:
    import socketserver as SocketServer
import threading
from pyspark.serializers import read_int, PickleSerializer


__all__ = ['Accumulator', 'AccumulatorParam']


pickleSer = PickleSerializer()

# Holds accumulators registered on the current machine, keyed by ID. This is then used to send
# the local accumulator updates back to the driver program at the end of a task.
_accumulatorRegistry = {}

def _deserialize_accumulator(aid, zero_value, accum_param):
    from pyspark.accumulators import _accumulatorRegistry
    # If this certain accumulator was deserialized, don't overwrite it.
    if aid in _accumulatorRegistry:
        return _accumulatorRegistry[aid]
    else:
        accum = Accumulator(aid, zero_value, accum_param)
        accum._deserialized = True
        _accumulatorRegistry[aid] = accum
        return accum

class AccumulatorMetadata(object):
    def __init__(self, metadata):
        self.id = metadata.id()
        self.name = metadata.name().toString()
        self.countFailedValues = metadata.countFailedValues()

    def getJavaMetadata(self):
        return sc._jvm.org.apache.spark.util.AccumulatorMetadata(
            self.id,
            self.name,
            self.countFailedValues)

class PythonAccumulatorListener(object):
    def __init__(self, gateway):
        self.gateway = gateway
    def merge(self, obj):
        print("Got something from java")
        for o in obj:
            deser = pickleSer.loads(o)
            print ("update for accum " + str(deser))
            aid = deser[0]
            update = deser[1] 
            _accumulatorRegistry[aid] += update
    class Java:
        implements = ["org.apache.spark.api.python.PythonAccumulatorListener"]

class Accumulator(object):

    """
    A shared variable that can be accumulated, i.e., has a commutative and associative "add"
    operation. Worker tasks on a Spark cluster can add values to an Accumulator with the C{+=}
    operator, but only the driver program is allowed to access its value, using C{value}.
    Updates from the workers get propagated automatically to the driver program.

    While C{SparkContext} supports accumulators for primitive data types like C{int} and
    C{float}, users can also define accumulators for custom types by providing a custom
    L{AccumulatorParam} object. Refer to the doctest of this module for an example.
    """

    def __init__(self, aid, value, accum_param):
        """Create a new Accumulator with a given initial value and AccumulatorParam object"""
        from pyspark.accumulators import _accumulatorRegistry
        self.aid = aid
        self.accum_param = accum_param
        self._value = value
        self._deserialized = False

    def register(self, sc, name):
        accum_type = AccumulatorType.CUSTOM_PYTHON_ACCUMULATOR # default
        # whether or not to attach a py4j callback listener to the 
        # jvm accumulator (only custom python accumulators, without jvm
        # counterparts)
        set_listener = True
        # if the param has a type defined, use that
        if hasattr(self.accum_param, "accum_type"):
            accum_type = self.accum_param.accum_type
            set_listener = False

        self._jac = getattr(sc._jvm, AccumulatorType.get_jvm_type(accum_type))()

        if set_listener:
            # Custom accumulators use the py4j Callback Server
            listener = PythonAccumulatorListener(sc._jvm)
            self._jac.setListener(listener)

        # get the scala SparkContext 
        ssc = sc._jsc.sc()
        acc_name = name if name != None else "python-accumulator-" + str(self.aid)
        ssc.register(self._jac, acc_name)
        self.aid = self._jac.id() # the accumulator will get a scala-side accumulator id
        _accumulatorRegistry[self.aid] = self
         # after this call, _jac has metadata populated
        self.accum_param.meta = AccumulatorMetadata(self._jac.metadata())

    def __reduce__(self):
        """Custom serialization; saves the zero value from our AccumulatorParam"""
        param = self.accum_param
        return (_deserialize_accumulator, (self.aid, param.zero(self._value), param))

    @property
    def value(self):
        """Get the accumulator's value; only usable in driver program"""
        if self._deserialized:
            raise Exception("Accumulator.value cannot be accessed inside tasks")
        # we get it from the JVM side
        val = self._jac.value()
        if (self.accum_param.accum_type == 2): # complex
            return complex(val.re(), val.im())
        else:
            return val

    @value.setter
    def value(self, value):
        """Sets the accumulator's value; only usable in driver program"""
        if self._deserialized:
            raise Exception("Accumulator.value cannot be accessed inside tasks")
        self._value = value

    def add(self, term):
        """Adds a term to this accumulator's value"""
        self._value = self.accum_param.addInPlace(self._value, term)

    def __iadd__(self, term):
        """The += operator; adds a term to this accumulator's value"""
        self.add(term)
        return self

    def __str__(self):
        return str(self._value)

    def __repr__(self):
        return "Accumulator<id=%i, value=%s>" % (self.aid, self._value)


class AccumulatorParam(object):

    """
    Helper object that defines how to accumulate values of a given type.
    """

    def zero(self, value):
        """
        Provide a "zero value" for the type, compatible in dimensions with the
        provided C{value} (e.g., a zero vector)
        """
        raise NotImplementedError

    def addInPlace(self, value1, value2):
        """
        Add two values of the accumulator's data type, returning a new value;
        for efficiency, can also update C{value1} in place and return it.
        """
        raise NotImplementedError


class AddingAccumulatorParam(AccumulatorParam):

    """
    An AccumulatorParam that uses the + operators to add values. Designed for simple types
    such as integers, floats, and lists. Requires the zero value for the underlying type
    as a parameter.
    """

    def __init__(self, zero_value, accum_type):
        self.zero_value = zero_value
        self.accum_type = accum_type

    def zero(self, value):
        return self.zero_value

    def addInPlace(self, value1, value2):
        value1 += value2
        return value1

class AccumulatorType(object):
    CUSTOM_PYTHON_ACCUMULATOR = -1
    LONG_ACCUMULATOR = 0
    DOUBLE_ACCUMULATOR = 1
    COMPLEX_ACCUMULATOR = 2
    
    python_to_jvm_map = {
        CUSTOM_PYTHON_ACCUMULATOR: "org.apache.spark.api.python.PythonAccumulatorV2",
        LONG_ACCUMULATOR: "org.apache.spark.util.LongAccumulator",
        DOUBLE_ACCUMULATOR: "org.apache.spark.util.DoubleAccumulator",
        COMPLEX_ACCUMULATOR: "org.apache.spark.util.ComplexAccumulator" 
    }

    @staticmethod
    def get_jvm_type(python_type):
        if python_type not in python_to_jvm_type:
            raise Exception(
                "Accumulator type {} doesn't have a JVM type registered."
                .format(python_type))
        return python_to_jvm_type[python_type]

# Singleton accumulator params for some standard types
INT_ACCUMULATOR_PARAM = AddingAccumulatorParam(0, AccumulatorType.LONG_ACCUMULATOR)
FLOAT_ACCUMULATOR_PARAM = AddingAccumulatorParam(0.0, AccumulatorType.DOUBLE_ACCUMULATOR)
COMPLEX_ACCUMULATOR_PARAM = AddingAccumulatorParam(0.0j, AccumulatorType.COMPLEX_ACCUMULATOR)

if __name__ == "__main__":
    import doctest
    (failure_count, test_count) = doctest.testmod()
    if failure_count:
        sys.exit(-1)
