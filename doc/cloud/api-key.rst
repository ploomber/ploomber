
Install the Cloud branch
========================
This is a private beta functionality, as such, the API might change in the future.
To get this functionality you'll have to upgrade to the latest version.

.. code-block:: console

    pip install ploomber --upgrade

You can then run a sanity check to make sure you have cloud support:

.. code-block:: console

    ploomber cloud --help

Getting your API key
====================

To generate an API key please these steps:

1. Go to the `cloud app <https://main.d3mpv0f3dqco4e.amplifyapp.com/>`_ .
2. Click on Start Now, and follow the registration flow, add your email and password.
3. Verify your email, you should receive a code to put into the portal along with your email.
4. Sign in with your credentials (if not directed to sign-in, follow the note below).
5. Now that you're inside, on the top left corner please click on Account and Get API Key.
6. The next step is setting this key in the Ploomber CLI, copy your key, and move to the next step.

*Note:  You can always go back to the login page and get your key from this page.

*Note:  Users who registered can log in directly by clicking on the top right corner menu & sign in.

Setting your API key
====================
To set your key, make sure you followed the steps above to **get a key (via registration)**.

You can simply run this command (make sure to set your key):

.. code-block:: console

    ploomber cloud set-key <YOUR_API_KEY>

You can validate that the key was set correctly by running the get command:

.. code-block:: console

    ploomber cloud get-key


From this point onwards you can use the cloud features.

**If you have any issues please reach out to us!**