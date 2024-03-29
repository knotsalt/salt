.. _peer:

==================
Peer Communication
==================

Salt 0.9.0 introduced the capability for Salt minions to publish commands. The
intent of this feature is not for Salt minions to act as independent brokers
one with another, but to allow Salt minions to pass commands to each other.

In Salt 0.10.0 the ability to execute runners from the master was added. This
allows for the master to return collective data from runners back to the
minions via the peer interface.

The peer interface is configured through two options in the master
configuration file. For minions to send commands from the master the ``peer``
configuration is used. To allow for minions to execute runners from the master
the ``peer_run`` configuration is used.

Since this presents a viable security risk by allowing minions access to the
master publisher the capability is turned off by default. The minions can be
allowed access to the master publisher on a per minion basis based on regular
expressions. Minions with specific ids can be allowed access to certain Salt
modules and functions.

Peer Configuration
==================

The configuration is done under the ``peer`` setting in the Salt master
configuration file, here are a number of configuration possibilities.

The simplest approach is to enable all communication for all minions, this is
only recommended for very secure environments.

.. code-block:: yaml

    peer:
      .*:
        - .*

This configuration allows minions with IDs ending in ``.example.com`` access
to the test, ps, and pkg module functions.

.. code-block:: yaml

    peer:
      .*\.example.com:
        - test\..*
        - ps\..*
        - pkg\..*


The configuration logic is simple, a regular expression is passed for matching
minion ids, and then a list of expressions matching minion functions is
associated with the named minion. For instance, this configuration will also
allow minions ending with foo.org access to the publisher.

.. code-block:: yaml

    peer:
      .*\.example.com:
        - test\..*
        - ps\..*
        - pkg\..*
      .*\.foo.org:
        - test\..*
        - ps\..*
        - pkg\..*

.. note::
    Functions are matched using regular expressions as well.

It is also possible to limit target hosts with the :term:`Compound Matcher`.
You can achieve this by adding another layer in between the source and the
allowed functions:

.. code-block:: yaml

    peer:
      '.*\.example\.com':
        - 'G@role:db':
          - test\..*
          - pkg\..*

.. note::

    Notice that the source hosts are matched by a regular expression
    on their minion ID, while target hosts can be matched by any of
    the :ref:`available matchers <targeting-compound>`.

    Note that globbing and regex matching on pillar values is not supported. You can only match exact values.


Peer Runner Communication
=========================

Configuration to allow minions to execute runners from the master is done via
the ``peer_run`` option on the master. The ``peer_run`` configuration follows
the same logic as the ``peer`` option. The only difference is that access is
granted to runner modules.

To open up access to all minions to all runners:

.. code-block:: yaml

    peer_run:
      .*:
        - .*

This configuration will allow minions with IDs ending in example.com access
to the manage and jobs runner functions.

.. code-block:: yaml

    peer_run:
      .*example.com:
        - manage.*
        - jobs.*

.. note::
    Functions are matched using regular expressions.

Using Peer Communication
========================

The publish module was created to manage peer communication. The publish module
comes with a number of functions to execute peer communication in different
ways. Currently there are three functions in the publish module. These examples
will show how to test the peer system via the salt-call command.

To execute test.version on all minions:

.. code-block:: bash

    # salt-call publish.publish \* test.version

To execute the manage.up runner:

.. code-block:: bash

    # salt-call publish.runner manage.up

To match minions using other matchers, use ``tgt_type``:

.. code-block:: bash

    # salt-call publish.publish 'webserv* and not G@os:Ubuntu' test.version tgt_type='compound'

.. note::
    In pre-2017.7.0 releases, use ``expr_form`` instead of ``tgt_type``.
