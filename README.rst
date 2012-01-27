pykafkap - Python Kafka Producer
================================

A simple Kafka producer client for Python.

Goals
-----

* Producer only
* Works as well for single connections as many connections in a threaded
  environment
* Connection Pooling
* Defensive network coding (short timeouts, tcp keepalives, connection
  recycling)
* Kafka 0.05 and 0.6 support
* Python 2.6 & 2.7 support (PyPy is bonus)

FAQ
---

Why the stupid name?
    So we don't try to make it do more than we need. Who's going to try to bolt
    a consumer onto something with an ugly name like "pykafkap"?


Yeah, but then why did you name the module "kafkap.py"?
    Putting "py" in the name of a Python module is redundant. Putting "py" in
    the name of a project namespaces it. See Q1.
