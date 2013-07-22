package ca.kijiji.contest;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

import com.lmax.disruptor.EventFactory;

/**
 * WARNING: This is a mutable object which will be recycled by the RingBuffer. You must take a copy of data it holds
 * before the framework recycles it.
 */
public final class ValueEvent {
    private String value;

	public static final String name = "([A-Z][A-Z][A-Z]+|ST [A-Z][A-Z][A-Z]+)";
	public static final Pattern namePattern = Pattern.compile(name);
	protected final Matcher nameMatcher = namePattern.matcher("");

    public String getValue() {
        return value;
    }

    public void setValue(String value) {
        this.value = value;
    }

    public final static EventFactory<ValueEvent> EVENT_FACTORY = new EventFactory<ValueEvent>() {
        public ValueEvent newInstance() {
            return new ValueEvent();
        }
    };
}