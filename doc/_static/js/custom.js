/*

I looked up how to do this directly using sphinx but found now answer in the
docs, then I took a look at the source code and found out that the HTML
is generated using Python, there isn't an easy way to add new blocks, this
is simpler
*/

function replaceAt(original, i, s) {
    return original.substr(0, i) + s + original.substr(i + s.length);
}

function addTerminalButtons(element){
    var btn_red = document.createElement('span');
    btn_red.setAttribute('class', 'circle red');
    element.appendChild(btn_red)

    var btn_red = document.createElement('span');
    btn_red.setAttribute('class', 'circle yellow');
    element.appendChild(btn_red)

    var btn_red = document.createElement('span');
    btn_red.setAttribute('class', 'circle green');
    element.appendChild(btn_red)
}


function addTerminalStyle(elements, title, buttons=false){
    var i;
    for (i = 0; i < elements.length; i++) {
        var element = elements[i]

        var top_div = document.createElement('div');
        top_div.setAttribute('class', 'terminal-top');
        element.insertBefore(top_div, element.childNodes[0])


        var btns_div = document.createElement('div');
        btns_div.setAttribute('class', 'btns');

        var title_div = document.createElement('div');
        title_div.setAttribute('class', 'title');

        if (element.id) {
            var idx = element.id.lastIndexOf('-')
            var id = replaceAt(element.id, idx, '.')
            title_div.textContent = title + ' (' + id + ')';
        } else {
            title_div.textContent = title;
        }

        top_div.appendChild(btns_div)
        top_div.appendChild(title_div)

        if (buttons) {
            addTerminalButtons(btns_div)
        }
    } 
}


$(document).ready(function () {
    bash = document.getElementsByClassName('highlight-console')
    addTerminalStyle(bash, "Terminal (shell)")

    ipython = document.getElementsByClassName('ipython')
    addTerminalStyle(ipython, "Terminal (ipython)")

    editor = document.getElementsByClassName('text-editor')
    addTerminalStyle(editor, "Text editor", buttons=true)
});
