Hooking custom logic
--------------------

* Mention that hooks are executed in the main process (if executor creates subprocesss)

`on_render`
-----------

Exceptions raised here will prevent execution


`on_render` user case: static analysis
--------------------------------------

`on_finish`
----------

`on_finish` user case: stating data assumptions
------------------------------------------------

Exceptions raised here prevent metadata from saving

`on_failure`
-----------

Exceptions here are silenced and shown as warnings

`on_failure` user case: notifications
-------------------------------------
