pykafkap - Python Kafka Producer
================================

A simple Kafka producer client for Python.

Goals
-----

* Producer only
* Simple single Connection API
* Connection Pooling
* Defensive network coding (short timeouts, tcp keepalives, connection
  recycling)
* Kafka 0.05 and 0.6 support
* Python 2.6 & 2.7 support (PyPy is bonus)

FAQ
---

Q. Why the stupid name?

A. So we don't try to make it do more than we need. Who's going to try to bolt
   a consumer onto something with an ugly name like "pykafkap"?


Q. Yeah, but then why did you name the module "kafkap.py"?

A. Putting "py" in the name of a Python module is redundant. Putting "py" in
   the name of a project namespaces it. See Q1.
