package net.joelinn.junit;

import org.junit.rules.MethodRule;
import org.junit.runners.model.FrameworkMethod;
import org.junit.runners.model.Statement;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author Joe Linn
 * 2/19/2018
 */
public class RetryRule implements MethodRule {
    private static final Logger log = LoggerFactory.getLogger(RetryRule.class);

    @Override
    public Statement apply(final Statement base, final FrameworkMethod method, Object target) {
        return new Statement() {
            @Override
            public void evaluate() throws Throwable {
                try {
                    base.evaluate();
                } catch (Throwable t) {
                    Retry retry = method.getAnnotation(Retry.class);
                    if (retry != null) {
                        log.warn("Test " + method.getName() + " failed initial run. Retrying.", t);
                        for (int i = 1; i <= retry.value(); i++) {
                            try {
                                base.evaluate();
                            } catch (Throwable innerThrowable) {
                                if (i >= retry.value()) {
                                    throw innerThrowable;
                                } else {
                                    log.warn("Test " + method.getName() + " failed on retry " + i, innerThrowable);
                                }
                            }
                        }
                    } else {
                        throw t;
                    }
                }
            }
        };
    }
}
