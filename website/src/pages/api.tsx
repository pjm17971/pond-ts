import Layout from '@theme/Layout';
import useBaseUrl from '@docusaurus/useBaseUrl';

import styles from './api.module.css';

export default function ApiPage(): JSX.Element {
  const referenceHref = useBaseUrl('/generated-api/index.html');

  return (
    <Layout
      title="API Reference"
      description="Generated TypeScript API reference for pond-ts"
    >
      <main className={styles.page}>
        <h1>API Reference</h1>
        <p className={styles.lede}>
          This is the generated reference layer for <code>pond-ts</code>. Use
          it alongside the hand-written guides when you want exact method
          signatures, type parameters, and return shapes.
        </p>
        <div className={styles.actions}>
          <a className={styles.button} href={referenceHref}>
            Open in a dedicated tab
          </a>
        </div>
        <div className={styles.frame}>
          <iframe src={referenceHref} title="pond-ts API reference" />
        </div>
      </main>
    </Layout>
  );
}
