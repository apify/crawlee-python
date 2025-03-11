import ThemedImage from "@theme/ThemedImage";
import React from "react";

import styles from "./HomepageCtaSection.module.css";
import homepageStyles from "../../pages/index.module.css";
import Button from "../Button";

export default function HomepageCtaSection() {
    return (
        <section className={styles.ctaSection}>
            <h2 className={styles.ctaTitle}>Get started now!</h2>
            <div className={styles.ctaDescription}>
                Crawlee won’t fix broken selectors for you (yet), but it makes
                building and maintaining reliable crawlers faster and easier—so
                you can focus on what matters most.
            </div>
            <div className={styles.ctaButtonContainer}>
                <Button to="/docs/quick-start" withIcon type="primary" isBig>
                    Get started
                </Button>
            </div>

            <div
                className={homepageStyles.fadedOutSeparator}
                id={styles.ctaFadedOutSeparator}
            />
            <div
                className={homepageStyles.fadedOutSeparatorVertical}
                id={styles.fadedOutSeparatorVerticalLeft}
            />
            <div
                className={homepageStyles.fadedOutSeparatorVertical}
                id={styles.fadedOutSeparatorVerticalRight}
            />
            <div
                className={homepageStyles.dashedDecorativeCircle}
                id={styles.ctaDashedCircleRight}
            />

            <ThemedImage
                className={styles.ctaImage}
                sources={{
                    light: "img/animated-crawlee-logo-light.svg",
                    dark: "img/animated-crawlee-logo-dark.svg",
                }}
            />
        </section>
    );
}
