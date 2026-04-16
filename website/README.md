# Pond docs site

This folder contains the Docusaurus documentation site for Pond.

## Local development

The docs site requires Node 20+.

```sh
source "$HOME/.nvm/nvm.sh"
nvm use 20
cd ..
npm ci
cd website
npm ci
npm start
```

## Production build

```sh
source "$HOME/.nvm/nvm.sh"
nvm use 20
cd ..
npm ci
cd website
npm run build
```

`npm run build` and `npm start` generate the TypeDoc API reference into `static/generated-api` before running Docusaurus.
Because that reference is generated from the root `src/` tree, the repo root dependencies need to be installed as well as the website dependencies.

## Deployment

The repo deploys the docs site to GitHub Pages from `.github/workflows/docs.yml`.
