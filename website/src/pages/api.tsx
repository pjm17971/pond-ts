import {useEffect} from 'react';

import Layout from '@theme/Layout';
import useBaseUrl from '@docusaurus/useBaseUrl';

import styles from './api.module.css';

export default function ApiPage(): JSX.Element {
  const referenceHref = useBaseUrl('/generated-api/index.html');

  useEffect(() => {
    window.location.replace(referenceHref);
  }, [referenceHref]);

  return (
    <Layout
      title="API Reference"
      description="Generated TypeScript API reference for Pond"
    >
      <main className={styles.page}>
        <div className={styles.card}>
          <h1>Pond API Reference</h1>
          <p className={styles.lede}>
            Redirecting to the generated full-width API reference.
          </p>
          <a className={styles.button} href={referenceHref}>
            Continue to the API reference
          </a>
        </div>
      </main>
    </Layout>
  );
}
