import Layout from '@theme/Layout';
import useBaseUrl from '@docusaurus/useBaseUrl';

import styles from './api.module.css';

export default function ApiPage(): JSX.Element {
  const coreHref = useBaseUrl('/generated-api/core/');
  const reactHref = useBaseUrl('/generated-api/react/');

  return (
    <Layout
      title="API Reference"
      description="Generated TypeScript API reference for pond-ts and @pond-ts/react"
    >
      <main className={styles.page}>
        <div className={styles.card}>
          <h1>API Reference</h1>
          <p className={styles.lede}>
            pond-ts and <code>@pond-ts/react</code> each have their own
            full-width generated reference. Pick the package whose API you want
            to browse.
          </p>
          <div className={styles.buttons}>
            <a className={styles.button} href={coreHref}>
              pond-ts (core)
            </a>
            <a className={styles.button} href={reactHref}>
              @pond-ts/react
            </a>
          </div>
        </div>
      </main>
    </Layout>
  );
}
