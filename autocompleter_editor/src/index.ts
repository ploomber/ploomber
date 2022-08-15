import {
  ILayoutRestorer,
  JupyterFrontEnd,
  JupyterFrontEndPlugin
} from '@jupyterlab/application';

import { ICommandPalette, MainAreaWidget, WidgetTracker } from '@jupyterlab/apputils';
import { CodeEditor, CodeEditorWrapper } from '@jupyterlab/codeeditor';
import { CodeMirrorEditor } from '@jupyterlab/codemirror';

/**
 * Initialization data for the autocompleter-editor extension.
 */
const plugin: JupyterFrontEndPlugin<void> = {
  id: 'autocompleter-editor:plugin',
  autoStart: true,
  requires: [ICommandPalette, ILayoutRestorer],
  activate: (app: JupyterFrontEnd, palette: ICommandPalette, restorer: ILayoutRestorer) => {
    console.log('Ploomber:: JupyterLab extension autocompleter-editor is activated!');

    //Add an application command to the command palette
    const command: string = 'ploomber::autocompleter-editor:open';
    app.commands.addCommand(command, {
      label: 'Text Editor',
      execute: () => {
        const editorFactory = (options: CodeEditor.IOptions) => {
            return new CodeMirrorEditor(options);
        };

        let model = new CodeEditor.Model();
        let content = new CodeEditorWrapper({model, factory: editorFactory});
        let widget = new MainAreaWidget({content});

        widget.id = 'ploomber-text-editor';
        widget.title.label = 'Text Editor';
        widget.title.closable = true;

        if (!tracker.has(widget)) {
          tracker.add(widget);
        }
        if (!widget.isAttached) {
          app.shell.add(widget, 'main');
        }
        app.shell.activateById(widget.id);

        completerKeyboardEvents(widget, content);
      }
    });
    palette.addItem({ command, category: 'editor' });

    let tracker = new WidgetTracker<MainAreaWidget>({
      namespace: 'text-editor'
    });

    restorer.restore(tracker, {
      command,
      name: () => 'text-editor'
    });

    // create the completer tooltip
    let body = document.getElementsByTagName("body")[0];
    let completerWidget = document.createElement('div');
    completerWidget.className = 'jp-Completer jp-HoverBox';
    completerWidget.id = 'completer-widget';
    body.appendChild(completerWidget);
    let ul = document.createElement('ul');
    ul.className = 'jp-Completer-list';
    ul.id = 'completer-list';
    completerWidget.appendChild(ul);
    completerWidget.style.display = "none";

    let currActiveOption = 0;

    let keywordsList = ['impScience', 'impMagic', 'imply', 'life', 'living', 'lifoo', 'danger', 'danish']; 
    
    // creates an entry in the completer tooltip
    function createCompleterEntry(keyword: string, isFirstEntry: boolean) {
      let li = document.createElement('li');
      if (isFirstEntry){
        li.className = "jp-Completer-item jp-mod-active";
      } else {
        li.className = "jp-Completer-item";
      }
      ul.appendChild(li);
      let code = document.createElement('code');
      code.className = "jp-Completer-match";
      code.innerHTML = keyword;
      li.appendChild(code);
    }

    // handles clicking outside and inside of the tooltip
    function completerClickEvents(e:MouseEvent) {
      if (completerWidget.style.display != "none" && !completerWidget.contains(e.target as Node)) {
        completerWidget.style.display = "none";
        document.removeEventListener('click', completerClickEvents);
      }
    } 

    let keys = {"Ctrl": false, "Space": false}; 

    function completerKeyboardEvents(widget: MainAreaWidget, content: CodeEditorWrapper) {
      let node = widget.node;
      let wordBeforeCursor = "";
      node.addEventListener("keydown", function(e) {
        let editor = content.editor;

        if (e.key == " ") {
          keys["Space"] = true;
        }

        if (e.key == "Control") {
          keys["Ctrl"] = true;
        }

        if (completerWidget.style.display == "none" && keys["Space"] == true && keys["Ctrl"] == true) {
          e.preventDefault();
          currActiveOption = 0;
          // find word before cursor
          let lineNum = editor.getSelection().start.line;
          let line = editor.getLine(lineNum);
          let endingInd = editor.getSelection().start.column;
          let lineTillCursor = line?.substring(0,endingInd+1);
          let wordArr = lineTillCursor?.split(" ");
          // uncomment for tabs and spaces
          // let wordArr = lineTillCursor?.split(/\s+/);
          if (wordArr)
              wordBeforeCursor = wordArr[wordArr?.length -1].trim();

          if (wordBeforeCursor != "") {
              e.preventDefault();
          } else {
              return;
          }

          // remove prior entries in the completer widget
          while (ul.firstChild) {
            ul.removeChild(ul.firstChild);
          }

          // get location of cursor
          let sidePanel = Array.from(document.getElementsByClassName("lm-Widget p-Widget lm-TabBar p-TabBar jp-SideBar jp-mod-left lm-BoxPanel-child p-BoxPanel-child")as HTMLCollectionOf<HTMLElement>);
          let sidePanelWidth = sidePanel[0].style.width.slice(0,-2);
          let cursors = Array.from(node?.getElementsByClassName('CodeMirror-cursor') as HTMLCollectionOf<HTMLElement>)
          for (let i = 0; i < cursors.length; i++) {
            if (cursors[i].style.height != '0px') {
              let cursorLocation = cursors[i].getBoundingClientRect();
              completerWidget.style.top = cursorLocation.top.toString() + "px";
              let leftPixel = parseInt(sidePanelWidth) + cursorLocation.left;
              completerWidget.style.left = leftPixel.toString() + "px";
            }
          }

          // searches for entries based on the word before the cursor
          let firstEntry = true;
          for (let i = 0; i < keywordsList.length; i++) {
            if (keywordsList[i].startsWith(wordBeforeCursor) && keywordsList[i] != wordBeforeCursor) {
              // creates an entry in the autocomplete tooltip
              if (firstEntry) {
                firstEntry = false;
                completerWidget.style.display = "flex";
                createCompleterEntry(keywordsList[i], true);
              }
              else 
                createCompleterEntry(keywordsList[i], false);
            } 
          }
          document.addEventListener("click", completerClickEvents);

          return;
        }

        if (completerWidget.style.display != "none") {
          e.preventDefault();

          // moves entries downwards
          if (keys["Space"] == true && keys["Ctrl"] == true) {
            ul.children[currActiveOption].classList.remove("jp-mod-active");
            if (currActiveOption < ul.children.length-1){
              currActiveOption += 1;
            } else {
              currActiveOption = 0;
            }
            ul.children[currActiveOption].classList.add("jp-mod-active");
          }
          // selects the entry
          else if (e.key == "Enter") {
            completerWidget.style.display = "none";
            document.removeEventListener('click', completerClickEvents);
            // replace cursor word with new word
            let textToReplace = ul.children[currActiveOption].textContent;

            if (textToReplace) {
              editor.replaceSelection?.(textToReplace.substring(wordBeforeCursor.length));
            }
          }
        }
      });

      node.addEventListener("keyup", function(e) {
        if (e.key == " ") {
          keys["Space"] = false;
        }

        if (e.key == "Control") {
          keys["Ctrl"] = false;
        }
      });
    }
  }
};

export default plugin;
