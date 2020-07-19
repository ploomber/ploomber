/*

I looked up how to do this directly using sphinx but found now answer in the
docs, then I took a look at the source code and found out that the HTML
is generated using Python, there isn't an easy way to add new blocks, this
is simpler
*/

function replaceAt(original, i, s) {
    return original.substr(0, i) + s + original.substr(i + s.length);
}

function addTerminalButtons(element) {
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


function addTerminalStyle(elements, title, buttons = false) {
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

        var copy_div = document.createElement('div');
        copy_div.setAttribute('class', 'copy-message');
        copy_div.textContent = "Click to copy"

        if (element.id) {
            var idx = element.id.lastIndexOf('-')
            var id = replaceAt(element.id, idx, '.')
            title_div.textContent = title + ' (' + id + ')';
        } else {
            title_div.textContent = title;
        }

        top_div.appendChild(btns_div)
        top_div.appendChild(title_div)
        top_div.appendChild(copy_div)

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
    addTerminalStyle(editor, "Text editor", buttons = true)


    // TODO: this should listen for click events in the whole terminal
    // window, not only in the content sub-window
    $("div.highlight").click(function () {
        navigator.clipboard.writeText($(this).text());
    });

    $(".highlight-python").hover(function () {
        $(this).find('.copy-message').css('opacity', 1)
    }, function () {
        $(this).find('.copy-message').css('opacity', 0)
        $(this).find('.copy-message').text('Click to copy')
    })

    $(".highlight-python").click(function () {
        $(this).find('.copy-message').text('Copied!')
    })

    updateCurrentSection()
});

function elementInViewport(el) {
    var top = el.offsetTop;
    var left = el.offsetLeft;
    var width = el.offsetWidth;
    var height = el.offsetHeight;

    while (el.offsetParent) {
        el = el.offsetParent;
        top += el.offsetTop;
        left += el.offsetLeft;
    }

    return (
        top < (window.pageYOffset + window.innerHeight) &&
        left < (window.pageXOffset + window.innerWidth) &&
        (top + height) > window.pageYOffset &&
        (left + width) > window.pageXOffset
    );
}

function findCurrentSection() {
    current = null
    $("div.section > div.section").each(function (i, section) {
        if (elementInViewport(section)) {
            current = $(this).find('a').attr('href')
            // break when you find the first one
            return false;
        }
    });

    return current
}

function updateCurrentSection() {
    current = findCurrentSection()
    $("div.sphinxsidebarwrapper a.reference").each(function () {
        if ($(this).attr('href') == current) {
            $(this).css('font-weight', 600)
        } else {
            $(this).css('font-weight', 100)
        }
    })
}

$(document).on("scroll", function () {
    updateCurrentSection()
});

