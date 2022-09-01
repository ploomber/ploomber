import { CodeMirrorEditor } from "@jupyterlab/codemirror";
import {IDisposable} from '@lumino/disposable';
import { CodeEditor} from '@jupyterlab/codeeditor';

let handlerDisposable: IDisposable; 

export class autocompleter {
    node: HTMLElement;
    editor: CodeMirrorEditor;
    ul: HTMLUListElement;
    completerWidget: HTMLDivElement;

    currActiveOption: number;
    keywordsList: string[];
    keys: any; 
    wordBeforeCursor: string;

    constructor(node: HTMLElement, editor: CodeMirrorEditor) {
        this.node = node;
        this.editor = editor;
        this.currActiveOption = 0;
        this.keywordsList= ['impScience', 'impMagic', 'imply', 'life', 'living', 'lifoo', 'danger', 'danish']; 
        this.keys = {"Ctrl": false, "Space": false}; 
        this.wordBeforeCursor = "";

        let body = document.getElementsByTagName("body")[0];
        this.completerWidget = document.createElement('div');
        this.completerWidget.className = 'jp-Completer jp-HoverBox';
        this.completerWidget.id = 'completer-widget';
        body.appendChild(this.completerWidget);
        this.ul = document.createElement('ul');
        this.ul.className = 'jp-Completer-list';
        this.ul.id = 'completer-list';
        this.completerWidget.appendChild(this.ul);
        this.completerWidget.style.display = "none";
    }

    // creates an entry in the completer tooltip
    createCompleterEntry(keyword: string, isFirstEntry: boolean):void {
        let li = document.createElement('li');
        if (isFirstEntry){
            li.className = "jp-Completer-item jp-mod-active";
        } else {
            li.className = "jp-Completer-item";
        }
        this.ul.appendChild(li);
        let code = document.createElement('code');
        code.className = "jp-Completer-match";
        code.innerHTML = keyword;
        li.appendChild(code);
    }

    // tooltip disappears if you if you click outside of the tooltip 
    completerClickEvents(e:MouseEvent): void {
        if (this.completerWidget.style.display != "none" && !this.completerWidget.contains(e.target as Node)) {
            this.completerWidget.style.display = "none";
            document.removeEventListener('click', this.completerClickEvents.bind(this));
            handlerDisposable.dispose();

        }
    } 

    completerKeyboardEvents():void {
        this.wordBeforeCursor = "";
        this.node.addEventListener("keydown", this.completerKeydownEvents.bind(this));
        this.node.addEventListener("keyup", this.trackIfAutocompleterKeysAreReleased.bind(this));
    }

    // tracks whether Ctrl or Space is released
    trackIfAutocompleterKeysAreReleased(e: KeyboardEvent) {
        if (e.key == " ") {
            this.keys["Space"] = false;
        }

        if (e.key == "Control") {
            this.keys["Ctrl"] = false;
        }
    }

    completerKeydownEvents(e:KeyboardEvent) {
        if (e.key == " ") {
            this.keys["Space"] = true;
        }

        if (e.key == "Control") {
            this.keys["Ctrl"] = true;
        }

        // if the completer tooltip has not yet appeared; first press of Ctrl + Space
        if (this.completerWidget.style.display == "none" && this.keys["Space"] == true && this.keys["Ctrl"] == true) {
            e.preventDefault();
            this.currActiveOption = 0;
            // find word before cursor
            let lineNum = this.editor.getSelection().start.line;
            let line = this.editor.getLine(lineNum);
            let endingInd = this.editor.getSelection().start.column - 1;
            let lineTillCursor = line?.substring(0,endingInd+1);
            // let wordArr = lineTillCursor?.split(" ");
            // uncomment for tabs and spaces
            let wordArr = lineTillCursor?.split(/\s+/);
            if (wordArr)
                this.wordBeforeCursor = wordArr[wordArr?.length -1].trim();

            if (this.wordBeforeCursor != "") {
                e.preventDefault();
            } else {
                return;
            }

            // remove prior entries in the completer widget
            while (this.ul.firstChild) {
                this.ul.removeChild(this.ul.firstChild);
            }

            // get location of cursor
            let sidePanel = Array.from(document.getElementsByClassName("lm-Widget p-Widget lm-TabBar p-TabBar jp-SideBar jp-mod-left lm-BoxPanel-child p-BoxPanel-child") as HTMLCollectionOf<HTMLElement>);
            let sidePanelWidth = sidePanel[0].style.width.slice(0,-2);
            let cursors = Array.from(this.node?.getElementsByClassName('CodeMirror-cursor') as HTMLCollectionOf<HTMLElement>)
            for (let i = 0; i < cursors.length; i++) {
                if (cursors[i].style.height != '0px') {
                    let cursorLocation = cursors[i].getBoundingClientRect();
                    this.completerWidget.style.top = cursorLocation.top.toString() + "px";
                    let leftPixel = parseInt(sidePanelWidth) + cursorLocation.left;
                    this.completerWidget.style.left = leftPixel.toString() + "px";
                }
            }

            // searches for entries based on the word before the cursor
            let firstEntry = true;
            for (let i = 0; i < this.keywordsList.length; i++) {
                if (this.keywordsList[i].startsWith(this.wordBeforeCursor) && this.keywordsList[i] != this.wordBeforeCursor) {
                    // creates an entry in the autocomplete tooltip
                    if (firstEntry) {
                        firstEntry = false;
                        this.completerWidget.style.display = "flex";
                        this.createCompleterEntry(this.keywordsList[i], true);
                    }
                    else 
                        this.createCompleterEntry(this.keywordsList[i], false);
                } 
            }

            // no words were found
            if (firstEntry) {
                return;
            }

            document.addEventListener("click", this.completerClickEvents.bind(this));

            const handler = (editor: CodeEditor.IEditor, event:KeyboardEvent) => {
                return event.key === "ArrowDown" || event.key === "ArrowUp" || event.key === "Enter";
            }
            
            handlerDisposable = this.editor.addKeydownHandler(handler);

            return;
        }

        // navigates the tooltip
        if (this.completerWidget.style.display != "none") {  
            e.preventDefault();
            e.stopPropagation();
            e.stopImmediatePropagation();        
            // moves entries downwards
            if (e.key == "ArrowDown") {
                this.ul.children[this.currActiveOption].classList.remove("jp-mod-active");
                if (this.currActiveOption < this.ul.children.length-1){
                    this.currActiveOption += 1;
                } else {
                    this.currActiveOption = 0;
                }
                this.ul.children[this.currActiveOption].classList.add("jp-mod-active");
            } 
            // moves entries upwards
            else if (e.key == "ArrowUp") {
                this.ul.children[this.currActiveOption].classList.remove("jp-mod-active");
                if (this.currActiveOption != 0) {
                    this.currActiveOption -= 1;
                } else {
                    this.currActiveOption = this.ul.children.length-1;
                }
                this.ul.children[this.currActiveOption].classList.add("jp-mod-active");
            } 
            // selects the entry
            else if (e.key == "Enter") {
                this.completerWidget.style.display = "none";
                document.removeEventListener("click", this.completerClickEvents.bind(this));
                handlerDisposable.dispose();
                // replace cursor word with new word
                let textToReplace = this.ul.children[this.currActiveOption].textContent;
                if (textToReplace) {
                    this.editor.replaceSelection?.(textToReplace.substring(this.wordBeforeCursor.length));
                }
            }
            // makes the tooltip disappear
            else if (e.key == "Escape" || e.key == "ArrowLeft") {
                this.completerWidget.style.display = "none";
                document.removeEventListener('click', this.completerClickEvents.bind(this));
                handlerDisposable.dispose();
            }
        }
    }
}