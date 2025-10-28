---
jupyter:
  jupytext:
    formats: ipynb,md
    text_representation:
      extension: .md
      format_name: markdown
      format_version: '1.3'
      jupytext_version: 1.16.1
  kernelspec:
    display_name: ds-aa-hti-hurricanes
    language: python
    name: ds-aa-hti-hurricanes
---

# Map plot legend move

```python
%load_ext jupyter_black
%load_ext autoreload
%autoreload 2
```

```python
from src.email.plotting import create_map_plot
```

```python
monitor_id = "al132025_fcast_2025-10-28T15:00:00"
```

```python
create_map_plot(monitor_id, "fcast")
```

```python

```
