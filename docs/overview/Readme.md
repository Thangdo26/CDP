# Overview document 

## Requirements 

* Install Quarto from <https://quarto.org/docs/get-started/>

* Install `tinytex` (to render to pdf format), under `bash` or `powershell` 

```
$ quarto install tinytex 
```

## Render 

```
$ cd overview
$ quarto render architecture.qmd --to pdf 
$ quarto render architecture.qmd --to html
$ quarto render architecture.qmd <-- all format 
```

## Preview

```
$ quarto preview architecture.qmd
```

## Quarto docs

* markdown: <https://quarto.org/docs/authoring/markdown-basics.html>
* html: <https://quarto.org/docs/reference/formats/html.html>
* pdf: <https://quarto.org/docs/reference/formats/pdf.html>

