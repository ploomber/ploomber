Setting Up
==========

Ploomber Cloud is in beta, and the API might change in the future.
To use it, you'll have to install Ploomber from git.

.. code-block:: console

    pip uninstall ploomber
    pip install git+https://github.com/ploomber/ploomber@cloud-stable

Getting API Key
***************

To generate an API key, follow these steps:

1. Go to the `cloud app <https://main.d3mpv0f3dqco4e.amplifyapp.com/register.html>`_.
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

Setting your API key
********************

You can run this command:

.. code-block:: console

    ploomber cloud set-key {your-key}

You can validate that the key was set correctly by running the get command:

.. code-block:: console

    ploomber cloud get-key

From this point onwards, you can use the cloud features.

**If you have any issues, please reach out to us!**
