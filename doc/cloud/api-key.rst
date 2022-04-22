Setting Up
==========

*Note: Ploomber Cloud is in beta*

Getting an API Key
******************

To generate an API key, follow these steps:

1. Go to the `cloud app <https://cloud.ploomber.io/register.html>`_.
2. Click on Start Now, follow the registration flow, and add your email and password.
3. Verify your email; you should receive a code to put into the portal along with your email.
4. Sign in with your credentials (if not directed to sign in, follow the note below).
5. Now that you're inside, please click on the text box blue link on the top left corner - API Key.

.. image:: https://ploomber.io/images/doc/cloud-key.png
   :target: https://ploomber.io/images/doc/cloud-key.png
   :alt: cloud-key-link-example

You'll see a pop-up where you can copy your key.

.. image:: https://ploomber.io/images/doc/cloud-key-modal.png
   :target: https://ploomber.io/images/doc/cloud-key-modal.png
   :alt: cloud-key-popup

6. The next step is setting this key in the Ploomber CLI: copy your key, and move to the next step.

*Note:  You can always go back to the login page and get your key from this page.*

*Note:  Users who registered can log in directly by clicking on the top right corner menu & sign in.*

Setting your API Key
********************

We're constantly improving Ploomber Cloud; ensure you're running the latest
version for the best experience: 

.. code-block:: console

    pip install ploomber --upgrade


Now, configure your API key:

.. code-block:: console

    ploomber cloud set-key {your-key}

You can validate that the key was set correctly by running the get command:

.. code-block:: console

    ploomber cloud get-key


You're now ready to use Ploomber Cloud! You use ``ploomber`` locally
and submit pipelines to the Cloud, or you can use a
hosted :ref:`JupyterLab <hosted-jupyterlab>` instance we provide for free,
so you don't have to set up an environment.

Check out the :doc:`following tutorial <guide>` to learn how to submit pipelines to the Cloud.

Or check the user guide to learn more about Ploomber Cloud's features.