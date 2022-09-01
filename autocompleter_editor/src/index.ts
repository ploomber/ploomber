import {
    ILayoutRestorer,
    JupyterFrontEnd,
    JupyterFrontEndPlugin
} from '@jupyterlab/application';

import { ICommandPalette, WidgetTracker } from '@jupyterlab/apputils';
import { autocompleter } from "./autocompleter";
import {DocumentWidget} from "@jupyterlab/docregistry";

const plugin: JupyterFrontEndPlugin<void> = {
  id: 'autocompleter-editor:plugin',
  autoStart: true,
  requires: [ICommandPalette, ILayoutRestorer],
  activate: (app: JupyterFrontEnd, palette: ICommandPalette, restorer: ILayoutRestorer) => {
    console.log('Ploomber:: JupyterLab extension autocompleter-editor is activated!');

    const openCommand: string = 'ploomber::autocompleter-editor:open';

    app.commands.addCommand(openCommand, {
      label: 'Ploomber Autocompleter Editor',
      execute: () => {
        app.commands
        .execute('docmanager:new-untitled', {
            path: "./",
            type: 'file',
            ext: ".yaml"
        })
        .then(model => {
            if (model != undefined) {
                app.commands.execute('docmanager:open', {
                    path: model.path,
                    factory: "editor"
                }).then((widget) => {
                    let node = widget.node;
                    let editor = widget._content.editor;

                    if (!tracker.has(widget)) {
                      tracker.add(widget);
                    }       
                    widget.context.pathChanged.connect(() => {
                      tracker.save(widget);
                    });
                    let autocomplete = new autocompleter(node, editor);
                    autocomplete.completerKeyboardEvents();
                });
            }
        });
      }
    });

    const refreshCommand: string = 'ploomber::autocompleter-editor:refresh';

    app.commands.addCommand(refreshCommand, {
      label: 'Refresh Editor',
      execute: (args) => {
        app.commands.execute('docmanager:open', {
          path: args.path,
          factory: "editor"
        }).then((widget) => {   
          let node = widget.node;
          let editor = widget._content.editor;
          let autocomplete = new autocompleter(node, editor);
          autocomplete.completerKeyboardEvents();
          widget.context.pathChanged.connect(() => {
            tracker.save(widget);
          });
        });
      }
    });

    palette.addItem({ command: openCommand, category: 'editor' });

    let tracker = new WidgetTracker<DocumentWidget>({
      namespace: 'text-editor'
    });

    if (restorer) {
      // When restoring the app, if the document was open, reopen it
      restorer.restore(tracker, {
          command: refreshCommand,
          args: (widget) => ({ path: widget.context.path, factory: "editor" }),
          name: (widget) => widget.context.path
      });
    }
  }
}

export default plugin;