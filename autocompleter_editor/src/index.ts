import {
  JupyterFrontEnd,
  JupyterFrontEndPlugin
} from '@jupyterlab/application';

/**
 * Initialization data for the autocompleter-editor extension.
 */
const plugin: JupyterFrontEndPlugin<void> = {
  id: 'autocompleter-editor:plugin',
  autoStart: true,
  activate: (app: JupyterFrontEnd) => {
    console.log('JupyterLab extension autocompleter-editor is activated!');
  }
};

export default plugin;
