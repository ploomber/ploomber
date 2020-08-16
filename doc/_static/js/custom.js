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

    $("[class*='highlight-']").hover(function () {
        $(this).find('.copy-message').css('opacity', 1)
    }, function () {
        $(this).find('.copy-message').css('opacity', 0)
        $(this).find('.copy-message').text('Click to copy')
    })

    $("[class*='highlight-']").click(function () {
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

