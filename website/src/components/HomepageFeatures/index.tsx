import type {ReactNode} from 'react';
import clsx from 'clsx';
import Heading from '@theme/Heading';
import styles from './styles.module.css';

type FeatureItem = {
  title: string;
  description: ReactNode;
};

const featureList: FeatureItem[] = [
  {
    title: 'Explicit temporal keys',
    description:
      'Work with Time, TimeRange, and Interval directly instead of treating time as an unnamed first column convention.',
  },
  {
    title: 'Typed analytical transforms',
    description:
      'Align, aggregate, join, roll, and smooth series while preserving schema-driven TypeScript types.',
  },
  {
    title: 'Modern deployment targets',
    description:
      'Designed for current Node and frontend projects with immutable values, ESM packaging, and timezone-aware ingest.',
  },
];

function Feature({title, description}: FeatureItem): ReactNode {
  return (
    <div className={clsx('col col--4', styles.featureCard)}>
      <div className={styles.featureInner}>
        <Heading as="h3">{title}</Heading>
        <p>{description}</p>
      </div>
    </div>
  );
}

export default function HomepageFeatures(): ReactNode {
  return (
    <section className={styles.features}>
      <div className="container">
        <div className="row">
          {featureList.map((feature) => (
            <Feature key={feature.title} {...feature} />
          ))}
        </div>
      </div>
    </section>
  );
}
