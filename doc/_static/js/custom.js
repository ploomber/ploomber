/*

I looked up how to do this directly using sphinx but found now answer in the
docs, then I took a look at the source code and found out that the HTML
is generated using Python, there isn't an easy way to add new blocks, this
is simpler
*/

function add_top_div(elements, title){
    var i;
    for (i = 0; i < elements.length; i++) {
        var top_div = document.createElement('div');
        top_div.textContent = title;
        top_div.setAttribute('class', 'terminal-top');
        elements[i].insertBefore(top_div, elements[i].childNodes[0])
    } 
}


$(document).ready(function () {
    bash = document.getElementsByClassName('highlight-bash')
    add_top_div(bash, "Terminal (shell)")

    bash = document.getElementsByClassName('ipython')
    add_top_div(bash, "Terminal (ipython)")
});
