function debounce(fn, delay) {
    let timerId = undefined;
    return function () {
        let ctx = this;
        let args = arguments;
        clearTimeout(timerId);
        timerId = setTimeout(() => {
            fn.apply(ctx, args);
        }, delay);
    }
}

debounce(function () {

}, 200);

function throttle(fn, delay) {
    let flag = true;

    return function () {
        if (flag) {
            let ctx = this;
            let args = arguments;
            fn.apply(ctx, args);
            flag = false;
            setTimeout(() => {
                flag = true;
            }, delay);
        }
    }
}