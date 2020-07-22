# DAG report

{% if plot %}

## Plot

{{plot}}

{% endif %}

{% if status %}
## Status

{{status}}

{% endif %}


{% if source %}
## Source code

{% for task in dag.values() %}

### {{task.name}}

```{{task.language}}
{{task.source}}
```
{% endfor %}

{% endif %}